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
package com.datastax.oss.pulsar.rabbitmqgw;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQDataBlockEncoder extends MessageToByteEncoder<AMQDataBlock> {

  private static final Logger log = LoggerFactory.getLogger(AMQDataBlockEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext ctx, AMQDataBlock amqDataBlock, ByteBuf byteBuf) {
    amqDataBlock.writePayload(
        new ByteBufferSender() {
          @Override
          public boolean isDirectBufferPreferred() {
            return true;
          }

          @Override
          public void send(QpidByteBuffer msg) {
            try {
              // TODO: verify that there's no copy here and no issue with lifecycle
              byteBuf.writeBytes(msg.asInputStream(), msg.remaining());
            } catch (Exception e) {
              log.error("Error while encoding message: {}", e.getMessage(), e);
              ctx.close();
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        });
  }
}
