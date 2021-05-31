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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.junit.jupiter.api.Test;

public class AMQChannelTest extends AbstractBaseTest {

  public static final String TEST_EXCHANGE = "test-exchange";
  public static final String DIRECT_EXCHANGE_TYPE = "direct";
  public static final String FANOUT_EXCHANGE_TYPE = "fanout";

  @Test
  void testReceiveChannelClose() {
    openConnection();
    sendChannelOpen();
    assertNotNull(connection.getChannel(CHANNEL_ID));

    AMQFrame frame = sendChannelClose();

    assertEquals(CHANNEL_ID, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ChannelCloseOkBody);
    assertNull(connection.getChannel(CHANNEL_ID));
  }

  @Test
  void testReceiveChannelCloseOk() {
    openConnection();
    sendChannelOpen();
    connection.markChannelAwaitingCloseOk(CHANNEL_ID);
    assertTrue(connection.channelAwaitingClosure(CHANNEL_ID));

    AMQFrame frame = sendChannelCloseOk();

    assertNull(frame);
    assertFalse(connection.channelAwaitingClosure(CHANNEL_ID));
  }

  @Test
  void testReceiveExchangeDeclare() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchange() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame =
        sendExchangeDeclare(ExchangeDefaults.DEFAULT_EXCHANGE_NAME, DIRECT_EXCHANGE_TYPE, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchangeInvalidType() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame =
        sendExchangeDeclare(ExchangeDefaults.DEFAULT_EXCHANGE_NAME, FANOUT_EXCHANGE_TYPE, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclarePassive() {
    openConnection();
    sendChannelOpen();
    sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, true);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclarePassiveNotFound() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, true);

    assertEquals(CHANNEL_ID, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ChannelCloseBody);
    ChannelCloseBody channelCloseBody = (ChannelCloseBody) body;
    assertEquals(ErrorCodes.NOT_FOUND, channelCloseBody.getReplyCode());
  }

  @Test
  void testReceiveExchangeDeclarePassiveInvalidType() {
    openConnection();
    sendChannelOpen();
    sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, FANOUT_EXCHANGE_TYPE, true);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclareReservedName() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame = sendExchangeDeclare("amq.test", DIRECT_EXCHANGE_TYPE, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeAlreadyExists() {
    openConnection();
    sendChannelOpen();
    sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeAlreadyExistsInvalidType() {
    openConnection();
    sendChannelOpen();
    sendExchangeDeclare(TEST_EXCHANGE, DIRECT_EXCHANGE_TYPE, false);

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, FANOUT_EXCHANGE_TYPE, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeInvalidType() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, "invalid-type", false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  private AMQFrame sendChannelClose() {
    ChannelCloseBody channelCloseBody = new ChannelCloseBody(0, AMQShortString.EMPTY_STRING, 0, 0);
    return exchangeData(channelCloseBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendExchangeDeclare(String exchange, String type, boolean passive) {
    ExchangeDeclareBody exchangeDeclareBody =
        new ExchangeDeclareBody(
            0,
            AMQShortString.createAMQShortString(exchange),
            AMQShortString.createAMQShortString(type),
            passive,
            true,
            false,
            false,
            false,
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    return exchangeData(exchangeDeclareBody.generateFrame(CHANNEL_ID));
  }

  private void assertIsExchangeDeclareOk(AMQFrame frame) {
    assertEquals(CHANNEL_ID, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ExchangeDeclareOkBody);
  }
}
