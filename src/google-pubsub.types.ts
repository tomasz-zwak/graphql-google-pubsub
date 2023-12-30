import { Message, Subscription } from "@google-cloud/pubsub";

export type GoogleSubAndClientIds = {
  sub?: Subscription;
  messageHandler?: (message: Message) => void;
  errorHandler?: CommonErrorHandler;
  ids?: Array<number>;
};

export type Topic2SubName = (
  topic: string,
  subscriptionOptions?: Object
) => string;

export type CommonMessageHandler = (message: Message) => Message;

export type CommonErrorHandler = (...args: any[]) => void;
