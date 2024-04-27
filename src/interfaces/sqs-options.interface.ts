import { Serializer, Deserializer } from '@nestjs/microservices';

export interface SQSOptions {
  /**
   * uri for AWS SQS
   * i.e. https://sqs.eu-west-1.amazonaws.com/account-id
   */
  sqsUri: string;

  /**
   * aws region for AWS SQS
   */
  region?: string;

  /**
   * API Version for AWS SQS
   */
  apiVersion?: string;

  /**
   * different profile than default for AWS SQS
   */
  awsProfile?: string;

  /**
   * long polling parameter
   * do not exceed 20
   */
  waitTimeSeconds?: number;

  /**
   * per sendMessage uniq id options  like MessageDeduplicationId, MessageGroupId
   */
  perSendMessageUniqIdOptions?: any;

  /**
   * instance of a class implementing the serialize method
   */
  serializer?: Serializer;

  /**
   * instance of a class implementing the deserialize method
   */
  deserializer?: Deserializer;
}

export interface AWSReadPacket {
  pattern: string;
  data: any;
  awsParameters: any;
}

export interface MessageHandler<T = any> {
  id: string;
  body: T;
}
