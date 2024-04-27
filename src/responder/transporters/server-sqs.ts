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

export class ServerSQS extends Server implements CustomTransportStrategy {
  private consumers: Map<string, Consumer>;

  constructor(
    private readonly options: SQSOptions,
    protected readonly logger: Logger,
  ) {
    super();

    this.initializeSerializer(options);
    this.initializeDeserializer(options);

    this.consumers = new Map<string, Consumer>();
  }

  public listen(callback: () => void) {
    this.bindHandlers();
    callback();
  }

  public bindHandlers() {
    this.messageHandlers.forEach((handler, queueName) =>
      this.bindHandler(handler, queueName),
    );
  }

  private bindHandler(
    handler: MessageHandler<any, any, any>,
    queueName: string,
  ) {
    if (handler.isEventHandler)
      return this.bindEventHandler(handler, queueName);
  }

  private bindEventHandler(
    handler: MessageHandler<any, any, any>,
    queueName: string,
  ) {
    const { sqsUri, waitTimeSeconds } = this.options;

    if (this.consumers.has(queueName)) return;

    const app = Consumer.create({
      queueUrl: `${sqsUri}/${queueName}`,
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

  public close() {
    this.consumers.forEach((consumer) => consumer.stop());
    this.consumers.clear();
  }
}
