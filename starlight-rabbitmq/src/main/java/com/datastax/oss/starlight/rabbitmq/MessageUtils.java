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

import java.util.Base64;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessageUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(AMQChannel.class);

  public static final String MESSAGE_PROPERTY_AMQP_HEADERS = "amqp-headers";
  public static final String MESSAGE_PROPERTY_AMQP_IMMEDIATE = "amqp-immediate";
  public static final String MESSAGE_PROPERTY_AMQP_MANDATORY = "amqp-mandatory";

  public static MessagePublishInfo getMessagePublishInfo(Message<byte[]> message) {
    String localName = TopicName.get(message.getTopicName()).getLocalName();
    String[] split = localName.split(".__", 2);
    String routingKey = "";
    String exchange = split[0];
    if (split.length > 1) {
      routingKey = split[1];
    }
    return new MessagePublishInfo(
        AMQShortString.createAMQShortString(exchange),
        Boolean.parseBoolean(message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_IMMEDIATE)),
        Boolean.parseBoolean(message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_MANDATORY)),
        AMQShortString.createAMQShortString(routingKey));
  }

  public static ContentHeaderBody getContentHeaderBody(Message<byte[]> message) {
    try {
      String headersProperty = message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS);
      if (headersProperty != null) {
        byte[] headers = Base64.getDecoder().decode(headersProperty);
        QpidByteBuffer buf = QpidByteBuffer.wrap(headers);
        return ContentHeaderBody.createFromBuffer(buf, headers.length);
      }
    } catch (AMQFrameDecodingException | IllegalArgumentException e) {
      LOGGER.error("Couldn't decode AMQP headers", e);
    }
    return new ContentHeaderBody(new BasicContentHeaderProperties(), message.size());
  }
}
