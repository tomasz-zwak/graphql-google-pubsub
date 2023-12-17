export type GoogleSubAndClientIds = {
  sub?: any; //
  messageHandler?: Function;
  errorHandler?: Function;
  ids?: Array<number>;
};

export type Topic2SubName = (
  topic: string,
  subscriptionOptions?: Object
) => string;

export type CommonMessageHandler = (message: any) => any;