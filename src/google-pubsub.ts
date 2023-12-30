import {
  Attributes,
  ClientConfig,
  CreateSubscriptionOptions,
  Message,
  PubSub,
} from "@google-cloud/pubsub";
import { PubSubEngine } from "graphql-subscriptions";

import { PubSubAsyncIterator } from "./async-iterator";
import {
  CommonErrorHandler,
  CommonMessageHandler,
  GoogleSubAndClientIds,
  Topic2SubName,
} from "./google-pubsub.types";

class NoSubscriptionOfIdError extends Error {
  constructor(subId: number) {
    super(`There is no subscription of id "${subId}"`);
  }
}

export class GooglePubSub implements PubSubEngine {
  private commonMessageHandler: CommonMessageHandler;
  private topic2SubName: Topic2SubName;
  public pubSubClient: PubSub;

  private clientId2GoogleSubNameAndClientCallback: {
    [clientId: number]: [string, Function];
  };
  private googleSubName2GoogleSubAndClientIds: {
    [topic: string]: GoogleSubAndClientIds;
  };

  private currentClientId: number;

  constructor(
    config?: ClientConfig,
    topic2SubName: Topic2SubName = (topicName) => `${topicName}-subscription`,
    commonMessageHandler: CommonMessageHandler = (message) => message,
    pubSubClient = new PubSub(config)
  ) {
    this.clientId2GoogleSubNameAndClientCallback = {};
    this.googleSubName2GoogleSubAndClientIds = {};
    this.currentClientId = 0;
    this.pubSubClient = pubSubClient;
    this.topic2SubName = topic2SubName;
    this.commonMessageHandler = commonMessageHandler;
  }

  async publish(topicName: string, data: any, attributes?: Attributes) {
    let _data = data;

    if (typeof data !== "string") {
      _data = JSON.stringify(data);
    }

    const topic = this.pubSubClient.topic(topicName);

    await topic.publishMessage({ data: Buffer.from(_data), attributes });
  }

  private async getSubscription(
    topicName: string,
    subName: string,
    options?: CreateSubscriptionOptions
  ) {
    const sub = this.pubSubClient.subscription(subName);
    const [exists] = await sub.exists();
    if (exists) {
      return sub;
    } else {
      const [newSub] = await this.pubSubClient
        .topic(topicName)
        .createSubscription(subName, options);
      return newSub;
    }
  }

  private getMessageHandler(subName: string) {
    return (message: Message) => {
      const res = this.commonMessageHandler(message);
      const { ids = [] } =
        this.googleSubName2GoogleSubAndClientIds[subName] || {};
      ids.forEach((id: number) => {
        const [, onMessage] = this.clientId2GoogleSubNameAndClientCallback[id];
        onMessage(res);
      });
      message.ack();
    };
  }

  async subscribe(
    topicName: string,
    onMessage: (...args: any[]) => void,
    options?: CreateSubscriptionOptions
  ): Promise<number> {
    const subName = this.topic2SubName(topicName, options);
    const id = this.currentClientId++;
    this.clientId2GoogleSubNameAndClientCallback[id] = [subName, onMessage];

    const { ids: oldIds = [], ...rest } =
      this.googleSubName2GoogleSubAndClientIds[subName] || {};
    this.googleSubName2GoogleSubAndClientIds[subName] = {
      ...rest,
      ids: [...oldIds, id],
    };
    if (oldIds.length > 0) return Promise.resolve(id);
    const sub = await this.getSubscription(topicName, subName, options);
    const googleSubAndClientIds =
      this.googleSubName2GoogleSubAndClientIds[subName] || {};
    // all clients have unsubscribed before the async subscription was created
    if (!googleSubAndClientIds.ids?.length) return id;
    const messageHandler = this.getMessageHandler(subName);
    // eslint-disable-next-line no-console
    const errorHandler: CommonErrorHandler = (error: unknown) =>
      console.error(error);

    sub.on("message", messageHandler);
    sub.on("error", errorHandler);

    this.googleSubName2GoogleSubAndClientIds[subName] = {
      ...googleSubAndClientIds,
      messageHandler,
      errorHandler,
      sub,
    };
    return id;
  }

  public unsubscribe(subId: number) {
    const [subName] = this.clientId2GoogleSubNameAndClientCallback[subId] || [
      undefined,
    ];
    if (!subName) throw new NoSubscriptionOfIdError(subId);
    const googleSubAndClientIds =
      this.googleSubName2GoogleSubAndClientIds[subName] || {};
    const { ids } = googleSubAndClientIds;

    if (!ids) throw new NoSubscriptionOfIdError(subId);

    if (ids.length === 1) {
      const { sub, messageHandler, errorHandler } = googleSubAndClientIds;
      // only remove listener if the client didn't unsubscribe before the subscription was created
      if (sub) {
        sub.removeListener("message", messageHandler);
        sub.removeListener("error", errorHandler);
      }

      delete this.googleSubName2GoogleSubAndClientIds[subName];
    } else {
      const index = ids.indexOf(subId);
      this.googleSubName2GoogleSubAndClientIds[subName] = {
        ...googleSubAndClientIds,
        ids:
          index === -1
            ? ids
            : [...ids.slice(0, index), ...ids.slice(index + 1)],
      };
    }
    delete this.clientId2GoogleSubNameAndClientCallback[subId];
  }

  public asyncIterator<T>(
    topics: string | string[],
    options?: any
  ): AsyncIterator<T> {
    return new PubSubAsyncIterator(this, topics, options);
  }
}
