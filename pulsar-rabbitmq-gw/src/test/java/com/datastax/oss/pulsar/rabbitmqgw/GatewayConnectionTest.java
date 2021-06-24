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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectBody;
import org.junit.jupiter.api.Test;

class GatewayConnectionTest extends AbstractBaseTest {

  @Test
  void testReceiveProtocolHeader() {
    AMQFrame frame = sendProtocolHeader();
    assertEquals(0, frame.getChannel());

    AMQBody body = frame.getBodyFrame();

    assertTrue(body instanceof ConnectionStartBody);
    ConnectionStartBody connectionStartBody = (ConnectionStartBody) body;
    assertEquals(0, connectionStartBody.getVersionMajor());
    assertEquals(9, connectionStartBody.getVersionMinor());
    assertEquals(0, connectionStartBody.getServerProperties().size());
    assertEquals("PLAIN", new String(connectionStartBody.getMechanisms(), StandardCharsets.UTF_8));
    assertEquals("en_US", new String(connectionStartBody.getLocales(), StandardCharsets.UTF_8));
  }

  @Test
  void testReceiveUnsupportedProtocolHeader() {
    // Send protocol header for unsupported v1.0
    ProtocolInitiation protocolInitiation =
        new ProtocolInitiation(ProtocolVersion.get((byte) 1, (byte) 0));

    ProtocolInitiation pi = exchangeData(protocolInitiation);

    assertEquals(9, pi.getProtocolMajor());
    assertEquals(1, pi.getProtocolMinor());
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionStartOk() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionStartOk("PLAIN");

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionTuneBody);
    ConnectionTuneBody connectionTuneBody = (ConnectionTuneBody) body;
    assertEquals(256, connectionTuneBody.getChannelMax());
    assertEquals(256 * 1024 - AMQFrame.getFrameOverhead(), connectionTuneBody.getFrameMax());
    assertEquals(0, connectionTuneBody.getHeartbeat());
  }

  @Test
  void testReceiveConnectionStartOkInvalidState() {
    sendProtocolHeader();
    sendConnectionStartOk("PLAIN");

    AMQFrame frame = sendConnectionStartOk("PLAIN");

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionStartOkEmptyMechanism() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionStartOk("");

    assertIsConnectionCloseFrame(frame, ErrorCodes.CONNECTION_FORCED);
  }

  @Test
  void testReceiveConnectionSecureOkInvalidState() {
    sendProtocolHeader();

    ConnectionSecureOkBody connectionSecureOkBody = new ConnectionSecureOkBody(new byte[0]);
    AMQFrame frame = exchangeData(connectionSecureOkBody.generateFrame(1));

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionTuneOk() {
    sendProtocolHeader();
    sendConnectionStartOk();

    AMQFrame frame = sendConnectionTuneOk();

    assertNull(frame);
    assertEquals(256, connection.getSessionCountLimit());
    assertEquals(128 * 1024, connection.getMaxFrameSize());
    assertEquals(60, connection.getHeartbeatDelay());
  }

  @Test
  void testReceiveConnectionTuneOkInvalidState() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionTuneOk();

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionTuneOkMaxFrameSizeTooBig() {
    sendProtocolHeader();
    sendConnectionStartOk();

    AMQFrame frame = sendConnectionTuneOk(256, 1024 * 1024, 60);

    assertIsConnectionCloseFrame(frame, ErrorCodes.SYNTAX_ERROR);
  }

  @Test
  void testReceiveConnectionTuneOkMaxFrameSizeTooSmall() {
    sendProtocolHeader();
    sendConnectionStartOk();

    AMQFrame frame = sendConnectionTuneOk(256, 1024, 60);

    assertIsConnectionCloseFrame(frame, ErrorCodes.SYNTAX_ERROR);
  }

  @Test
  void testReceiveConnectionTuneOkMaxFrameSizeImplied() {
    sendProtocolHeader();
    sendConnectionStartOk();

    sendConnectionTuneOk(256, 0, 60);

    assertEquals(256 * 1024 - AMQFrame.getFrameOverhead(), connection.getMaxFrameSize());
  }

  @Test
  void testReceiveConnectionTuneOkChannelMaxImplied() {
    sendProtocolHeader();
    sendConnectionStartOk();

    sendConnectionTuneOk(0, 128 * 1024, 60);

    assertEquals(0xFFFF, connection.getSessionCountLimit());
  }

  @Test
  void testReceiveConnectionOpen() {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk();

    AMQFrame frame = sendConnectionOpen("test-vhost");

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionOpenOkBody);
    ConnectionOpenOkBody connectionOpenOkBody = (ConnectionOpenOkBody) body;
    assertEquals("test-vhost", connectionOpenOkBody.getKnownHosts().toString());
    assertEquals("public/test-vhost", connection.getNamespace());
  }

  @Test
  void testReceiveConnectionOpenVhostWithSlash() {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk();

    AMQFrame frame = sendConnectionOpen("/test-vhost");

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionOpenOkBody);
    ConnectionOpenOkBody connectionOpenOkBody = (ConnectionOpenOkBody) body;
    assertEquals("/test-vhost", connectionOpenOkBody.getKnownHosts().toString());
    assertEquals("public/test-vhost", connection.getNamespace());
  }

  @Test
  void testReceiveConnectionOpenEmptyVhost() {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk();

    AMQFrame frame = sendConnectionOpen("/");

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionOpenOkBody);
    ConnectionOpenOkBody connectionOpenOkBody = (ConnectionOpenOkBody) body;
    assertEquals("/", connectionOpenOkBody.getKnownHosts().toString());
    assertEquals("public/default", connection.getNamespace());
  }

  @Test
  void testReceiveConnectionOpenInvalidState() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionOpen();

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionClose() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionClose();

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionCloseOkBody);
    ConnectionCloseOkBody connectionCloseOkBody = (ConnectionCloseOkBody) body;
    assertEquals(CONNECTION_CLOSE_OK_0_9.getMethod(), connectionCloseOkBody.getMethod());
    assertFalse(channel.isOpen());
  }

  @Test
  void testReceiveConnectionCloseOk() {
    sendProtocolHeader();

    AMQFrame frame = sendConnectionCloseOk();

    assertNull(frame);
    assertFalse(channel.isOpen());
  }

  @Test
  void testSendConnectionCloseTimeout() throws Exception {
    config.setAmqpConnectionCloseTimeout(100);
    connection.sendConnectionClose(ErrorCodes.NOT_IMPLEMENTED, "test message", CHANNEL_ID);

    channel.readOutbound();
    assertTrue(channel.isOpen());

    Thread.sleep(101);
    channel.runPendingTasks();
    assertFalse(channel.isOpen());
  }

  @Test
  void testHeartbeatSentIfIdle() throws Exception {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk(256, 128 * 1024, 1);

    Thread.sleep(1001);
    channel.runPendingTasks();
    AMQFrame frame = channel.readOutbound();

    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof HeartbeatBody);
  }

  @Test
  void testHeartbeatTimeout() throws Exception {
    sendProtocolHeader();
    sendConnectionStartOk();
    config.setAmqpHeartbeatTimeoutFactor(1);
    sendConnectionTuneOk(256, 128 * 1024, 1);

    Thread.sleep(1001);
    channel.runPendingTasks();

    assertFalse(channel.isOpen());
  }

  @Test
  void testHeartbeat() throws Exception {
    sendProtocolHeader();
    sendConnectionStartOk();
    config.setAmqpHeartbeatTimeoutFactor(1);
    sendConnectionTuneOk(256, 128 * 1024, 1);

    Thread.sleep(600);
    channel.runPendingTasks();
    exchangeData(HeartbeatBody.FRAME);

    Thread.sleep(600);
    channel.runPendingTasks();

    assertTrue(channel.isOpen());
  }

  @Test
  void testReceiveChannelOpen() {
    openConnection();
    assertNull(connection.getChannel(CHANNEL_ID));

    AMQFrame frame = sendChannelOpen();

    assertEquals(CHANNEL_ID, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ChannelOpenOkBody);
    assertNotNull(connection.getChannel(CHANNEL_ID));
  }

  @Test
  void testReceiveChannelOpenInvalidState() {
    sendProtocolHeader();

    AMQFrame frame = sendChannelOpen();

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  @Test
  void testReceiveChannelOpenAlreadyExists() {
    openConnection();

    sendChannelOpen();
    AMQFrame frame = sendChannelOpen();

    assertIsConnectionCloseFrame(frame, ErrorCodes.CHANNEL_ERROR);
  }

  // TODO testReceiveChannelOpenAlreadyExistsAwaitingClosure
  /* @Test
  void testReceiveChannelOpenAlreadyExistsAwaitingClosure() {
  }*/

  @Test
  void testReceiveChannelIdTooBig() {
    openConnection();

    AMQFrame frame = sendChannelOpen(1024);

    assertIsConnectionCloseFrame(frame, ErrorCodes.CHANNEL_ERROR);
  }

  @Test
  void testReceiveChannelFrameInvalidState() {
    sendProtocolHeader();

    AMQFrame frame = sendChannelCloseOk();

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  @Test
  void testReceiveChannelFrameChannelNotFound() {
    openConnection();

    AMQFrame frame = sendChannelCloseOk();

    assertIsConnectionCloseFrame(frame, ErrorCodes.CHANNEL_ERROR);
  }

  @Test
  void testReceiveChannelCloseOkWhileConnectionClosing() {
    openConnection();
    sendChannelOpen();
    connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "", 0);

    assertTrue(connection.channelAwaitingClosure(CHANNEL_ID));

    sendChannelCloseOk();

    assertTrue(connection.channelAwaitingClosure(CHANNEL_ID));
  }

  @Test
  void testReceiveChannelFrameWhileConnectionClosing() {
    openConnection();

    AMQFrame frame = exchangeData(TxSelectBody.INSTANCE.generateFrame(CHANNEL_ID));

    assertIsConnectionCloseFrame(frame, ErrorCodes.CHANNEL_ERROR);
  }
}
