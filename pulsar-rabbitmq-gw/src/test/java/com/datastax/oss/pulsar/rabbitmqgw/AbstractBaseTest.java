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

import static org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody.CONNECTION_CLOSE_OK_0_9;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Collections;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.junit.jupiter.api.BeforeEach;

public class AbstractBaseTest {
  public static final int CHANNEL_ID = 42;
  protected final GatewayConfiguration config = new GatewayConfiguration();
  protected final GatewayService gatewayService = spy(new GatewayService(config));
  protected final GatewayConnection connection = new GatewayConnection(gatewayService);
  protected EmbeddedChannel channel;

  @BeforeEach
  void setup() {
    channel = new EmbeddedChannel(connection, new AMQDataBlockEncoder());
  }

  protected AMQFrame exchangeData(AMQDataBlock data) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    data.writePayload(new NettyByteBufferSender(byteBuf));
    channel.writeInbound(byteBuf);
    return channel.readOutbound();
  }

  protected void openConnection() {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk();
    sendConnectionOpen();
  }

  protected AMQFrame sendProtocolHeader() {
    return exchangeData(new ProtocolInitiation(ProtocolVersion.v0_91));
  }

  protected AMQFrame sendConnectionStartOk() {
    return sendConnectionStartOk("PLAIN");
  }

  protected AMQFrame sendConnectionStartOk(String mechanism) {
    ConnectionStartOkBody connectionStartOkBody =
        new ConnectionStartOkBody(
            FieldTable.convertToFieldTable(Collections.emptyMap()),
            AMQShortString.createAMQShortString(mechanism),
            new byte[0],
            AMQShortString.createAMQShortString("en_US"));
    return exchangeData(connectionStartOkBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionTuneOk() {
    return sendConnectionTuneOk(256, 128 * 1024, 60);
  }

  protected AMQFrame sendConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
    ConnectionTuneOkBody connectionTuneOkBody =
        new ConnectionTuneOkBody(channelMax, frameMax, heartbeat);
    return exchangeData(connectionTuneOkBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionOpen() {
    return sendConnectionOpen("");
  }

  protected AMQFrame sendConnectionOpen(String vhost) {
    ConnectionOpenBody connectionOpenBody =
        new ConnectionOpenBody(
            AMQShortString.createAMQShortString(vhost),
            AMQShortString.createAMQShortString("test-capabilities"),
            false);
    return exchangeData(connectionOpenBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionClose() {
    ConnectionCloseBody connectionCloseBody =
        new ConnectionCloseBody(
            ProtocolVersion.v0_91,
            ErrorCodes.INTERNAL_ERROR,
            AMQShortString.createAMQShortString("test-replyText"),
            43,
            44);
    return exchangeData(connectionCloseBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionCloseOk() {
    return exchangeData(CONNECTION_CLOSE_OK_0_9.generateFrame(1));
  }

  protected AMQFrame sendChannelOpen() {
    return sendChannelOpen(CHANNEL_ID);
  }

  protected AMQFrame sendChannelOpen(int channelId) {
    ChannelOpenBody channelOpenBody = new ChannelOpenBody();
    return exchangeData(channelOpenBody.generateFrame(channelId));
  }

  protected AMQFrame sendChannelCloseOk() {
    return exchangeData(ChannelCloseOkBody.INSTANCE.generateFrame(CHANNEL_ID));
  }

  protected void assertIsConnectionCloseFrame(AMQFrame frame, int errorCode) {
    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionCloseBody);
    ConnectionCloseBody connectionCloseBody = (ConnectionCloseBody) body;
    assertEquals(errorCode, connectionCloseBody.getReplyCode());
  }

  public static class NettyByteBufferSender implements ByteBufferSender {

    private final ByteBuf byteBuf;

    NettyByteBufferSender(ByteBuf byteBuf) {
      this.byteBuf = byteBuf;
    }

    @Override
    public boolean isDirectBufferPreferred() {
      return true;
    }

    @Override
    public void send(QpidByteBuffer msg) {
      try {
        byteBuf.writeBytes(msg.asInputStream(), msg.remaining());
      } catch (Exception e) {
        // Oops
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
