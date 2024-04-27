import { Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { v4 as uuidv4 } from 'uuid';
import { Producer } from 'sqs-producer';

import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

export class ClientSQS extends ClientProxy {
  protected readonly logger: Logger;
  protected producers: Map<string, Producer>;
  protected sqsClient: SQSClient;

  constructor(protected readonly options: SQSOptions) {
    super();

    this.initializeDeserializer(options);
    this.initializeSerializer(options);

    this.producers = new Map<string, Producer>();
    this.logger = new Logger(ClientProxy.name);

    this.sqsClient = new SQSClient();
  }

  protected publish(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => any,
  ): any {}

  async dispatchEvent(packet: ReadPacket): Promise<any> {
    const pattern = this.normalizePattern(packet.pattern);

    let producer = this.producers.get(pattern);

    if (!producer) {
      let queueUrl: string;

      try {
        queueUrl = await this.getQueueUrl(pattern);
      } catch (error) {
        await this.createQueue(pattern);
        queueUrl = await this.getQueueUrl(pattern);
      }

      producer = Producer.create({
        queueUrl: queueUrl,
      });

      this.producers.set(pattern, producer);
    }

    return await producer.send({
      id: uuidv4(),
      body: JSON.stringify(packet.data),
    });
  }

  async getQueueUrl(pattern: string) {
    const getQueueUrlCommand = new GetQueueUrlCommand({
      QueueName: pattern,
    });

    const { QueueUrl } = await this.sqsClient.send(getQueueUrlCommand);

    return QueueUrl;
  }

  async createQueue(pattern: string) {
    const createQueueCommand = new CreateQueueCommand({
      QueueName: pattern,
      Attributes: {
        VisibilityTimeout: '60',
        MessageRetentionPeriod: '1209600',
      },
    });

    await this.sqsClient.send(createQueueCommand);
  }

  public async connect() {
    const { sqsUri, region } = this.options;

    if (!sqsUri) throw new Error('Unable to init AWS SQS, missing sqsUri');
    if (!region) throw new Error('Unable to init AWS SQS, missing region');
  }

  public close() {
    this.producers = new Map<string, Producer>();
  }
}
