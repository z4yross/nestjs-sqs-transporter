import { Logger } from '@nestjs/common';
import { ReadPacket, Serializer } from '@nestjs/microservices';

export class OutboundMessageIdentitySerializer implements Serializer {
  private readonly logger = new Logger('OutboundMessageIdentitySerializer');

  serialize(value: ReadPacket): ReadPacket {
    this.logger.debug(
      `-->> Serializing outbound response: \n${JSON.stringify(value)}`,
    );
    return value;
  }
}
