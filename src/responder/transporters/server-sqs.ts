import {
  Server,
  CustomTransportStrategy,
  MessageHandler,
} from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { Consumer } from 'sqs-consumer';
import { Message } from 'sqs-producer';
import { isObservable } from 'rxjs';
import { Logger } from '@nestjs/common';

import * as crypto from 'crypto';
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

export class ServerSQS extends Server implements CustomTransportStrategy {
  private consumers: Map<string, Consumer>;

  private sqsClient: SQSClient;

  constructor(
    private readonly options: SQSOptions,
    protected readonly logger: Logger,
  ) {
    super();

    this.initializeSerializer(options);
    this.initializeDeserializer(options);

    this.consumers = new Map<string, Consumer>();

    this.sqsClient = new SQSClient();
  }

  public async listen(callback: () => void) {
    await this.bindHandlers();
    callback();
  }

  public async bindHandlers() {
    for (const [queueName, handler] of this.messageHandlers) {
      await this.bindHandler(handler, queueName);
    }
  }

  private async bindHandler(
    handler: MessageHandler<any, any, any>,
    queueName: string,
  ) {
    if (handler.isEventHandler)
      return await this.bindEventHandler(handler, queueName);
  }

  private async bindEventHandler(
    handler: MessageHandler<any, any, any>,
    queueName: string,
  ) {
    const { sqsUri, waitTimeSeconds } = this.options;

    if (this.consumers.has(queueName)) return;

    let queueUrl: string;

    try {
      queueUrl = await this.getQueueUrl(queueName);
    } catch (error) {
      this.logger.debug(`Queue ${queueName} not found, creating it...`);
      queueUrl = await this.createQueue(queueName);
    }

    const app = Consumer.create({
      queueUrl: queueUrl,
      waitTimeSeconds: waitTimeSeconds ?? 0,

      handleMessage: async (data: unknown) => {
        const message = data as Message;
        const streamOrResult = await handler(JSON.parse(message.body));
        if (isObservable(streamOrResult)) streamOrResult.subscribe();
      },
    });

    app.on('error', (err) => {
      this.logger.error(`Error while consuming SQS: ${err.message}`);
    });

    app.on('processing_error', (err) => {
      this.logger.error(`Error while processing SQS: ${err.message}`);
    });

    app.start();
    this.consumers.set(queueName, app);
  }

  async getQueueUrl(pattern: string) {
    const md5Pattern = crypto.createHash('md5').update(pattern).digest('hex');

    const getQueueUrlCommand = new GetQueueUrlCommand({
      QueueName: md5Pattern,
    });

    const { QueueUrl } = await this.sqsClient.send(getQueueUrlCommand);

    this.logger.debug(`Queue URL for ${pattern} is ${QueueUrl}`);

    return QueueUrl;
  }

  async createQueue(pattern: string) {
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

  public close() {
    this.consumers.forEach((consumer) => consumer.stop());
    this.consumers.clear();
  }
}
