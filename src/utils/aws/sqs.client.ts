import * as crypto from 'crypto';
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { Logger } from '@nestjs/common';

export class AWSSQSClient {
  private static instance: AWSSQSClient;
  private sqsClient: SQSClient;
  private logger: Logger;

  private constructor() {
    this.sqsClient = new SQSClient({});
    this.logger = new Logger(AWSSQSClient.name);
  }

  public static getInstance(): AWSSQSClient {
    if (!AWSSQSClient.instance) {
      AWSSQSClient.instance = new AWSSQSClient();
    }

    return AWSSQSClient.instance;
  }

  public async getQueueUrl(pattern: string) {
    const md5Pattern = crypto.createHash('md5').update(pattern).digest('hex');

    const getQueueUrlCommand = new GetQueueUrlCommand({
      QueueName: md5Pattern,
    });

    const { QueueUrl } = await this.sqsClient.send(getQueueUrlCommand);

    this.logger.debug(`Queue URL for ${pattern} is ${QueueUrl}`);

    return QueueUrl;
  }

  public async createQueue(pattern: string) {
    const md5Pattern = crypto.createHash('md5').update(pattern).digest('hex');

    const createQueueCommand = new CreateQueueCommand({
      QueueName: md5Pattern,
      Attributes: {
        VisibilityTimeout: '60',
        MessageRetentionPeriod: '1209600',
      },
    });

    const response = await this.sqsClient.send(createQueueCommand);

    this.logger.debug(
      `Creating queue ${pattern} with URL ${response.QueueUrl}`,
    );

    return response.QueueUrl;
  }
}
