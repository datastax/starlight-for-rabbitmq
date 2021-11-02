/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.starlight.rabbitmq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.junit.jupiter.api.Test;

class AMQDataBlockEncoderTest {

  @Test
  void encodeAMQDataBlock() {
    EmbeddedChannel channel = new EmbeddedChannel(new AMQDataBlockEncoder());

    ProtocolInitiation protocolInitiation = new ProtocolInitiation(ProtocolVersion.v0_91);
    channel.writeOutbound(protocolInitiation);
    ByteBuf encodedBuf = channel.readOutbound();

    assertEquals(8, encodedBuf.readableBytes());
    assertEquals("AMQP", encodedBuf.readCharSequence(4, StandardCharsets.UTF_8).toString());
    assertEquals(0, encodedBuf.readByte());
    assertEquals(0, encodedBuf.readByte());
    assertEquals(9, encodedBuf.readByte());
    assertEquals(1, encodedBuf.readByte());
  }
}
