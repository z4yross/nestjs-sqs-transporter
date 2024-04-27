import { Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { v4 as uuidv4 } from 'uuid';
import { Producer } from 'sqs-producer';

import * as crypto from 'crypto';

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
        this.logger.debug(`Queue ${pattern} not found, creating it...`);
        queueUrl = await this.createQueue(pattern);
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

  public async connect() {
    const { sqsUri, region } = this.options;

    if (!sqsUri) throw new Error('Unable to init AWS SQS, missing sqsUri');
    if (!region) throw new Error('Unable to init AWS SQS, missing region');
  }

  public close() {
    this.producers = new Map<string, Producer>();
  }
}
