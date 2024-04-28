import { Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { v4 as uuidv4 } from 'uuid';
import { Producer } from 'sqs-producer';

import { AWSSQSClient } from '@utils/aws/sqs.client';

export class ClientSQS extends ClientProxy {
  protected readonly logger: Logger;
  protected producers: Map<string, Producer>;
  protected awsSqsClient: AWSSQSClient;

  constructor(protected readonly options: SQSOptions) {
    super();

    this.initializeDeserializer(options);
    this.initializeSerializer(options);

    this.producers = new Map<string, Producer>();
    this.logger = new Logger(ClientProxy.name);
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
        queueUrl = await this.awsSqsClient.getQueueUrl(pattern);
      } catch (error) {
        this.logger.debug(`Queue ${pattern} not found, creating it...`);
        queueUrl = await this.awsSqsClient.createQueue(pattern);
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

  public async connect() {
    const { sqsUri } = this.options;

    if (!sqsUri) throw new Error('Unable to init AWS SQS, missing sqsUri');

    this.awsSqsClient = AWSSQSClient.getInstance();
  }

  public close() {
    this.producers = new Map<string, Producer>();
  }
}
