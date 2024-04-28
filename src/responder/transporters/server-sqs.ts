import {
  Server,
  CustomTransportStrategy,
  MessageHandler,
  Transport,
} from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { Consumer } from 'sqs-consumer';
import { Message } from 'sqs-producer';
import { isObservable } from 'rxjs';
import { Logger } from '@nestjs/common';
import { AWSSQSClient } from '@utils/aws/sqs.client';
import { SQS_TRANSPORTER } from '@consts';

export class ServerSQS extends Server implements CustomTransportStrategy {
  private consumers: Map<string, Consumer>;
  private awsSqsClient: AWSSQSClient;
  readonly transportId?: Transport | symbol;

  constructor(
    private readonly options: SQSOptions,
    protected readonly logger: Logger,
    transportId?: Transport | symbol,
  ) {
    super();

    this.initializeSerializer(options);
    this.initializeDeserializer(options);

    this.consumers = new Map<string, Consumer>();
    this.awsSqsClient = AWSSQSClient.getInstance();

    this.transportId = transportId || SQS_TRANSPORTER;
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
    if (this.consumers.has(queueName)) return;

    let queueUrl: string;

    try {
      queueUrl = await this.awsSqsClient.getQueueUrl(queueName);
    } catch (error) {
      this.logger.debug(`Queue ${queueName} not found, creating it...`);
      queueUrl = await this.awsSqsClient.createQueue(queueName);
    }

    const app = Consumer.create({
      queueUrl: queueUrl,

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

  public close() {
    this.consumers.forEach((consumer) => consumer.stop());
    this.consumers.clear();
  }
}
