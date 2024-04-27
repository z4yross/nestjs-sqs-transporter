import { Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';

import { SQSOptions } from '../../interfaces/sqs-options.interface';

import { v4 as uuidv4 } from 'uuid';
import { Producer } from 'sqs-producer';

export class ClientSQS extends ClientProxy {
  protected readonly logger: Logger;
  protected producers: Map<string, Producer>;

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

  protected async dispatchEvent(packet: ReadPacket): Promise<any> {
    const pattern = this.normalizePattern(packet.pattern);

    let producer = this.producers.get(pattern);

    if (!producer) {
      producer = Producer.create({
        queueUrl: `${this.options.sqsUri}/${pattern}`,
      });

      this.producers.set(pattern, producer);
    }

    try {
      await producer.send({
        id: uuidv4(),
        body: JSON.stringify(packet.data),
      });
    } catch (err) {
      this.logger.error(err);
    }
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
