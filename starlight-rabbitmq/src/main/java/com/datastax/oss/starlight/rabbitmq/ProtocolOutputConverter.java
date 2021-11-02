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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageContentSource;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQVersionAwareProtocolSession;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.util.GZIPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapted from {@link org.apache.qpid.server.protocol.v0_8.ProtocolOutputConverterImpl} */
public class ProtocolOutputConverter {
  private static final int BASIC_CLASS_ID = 60;
  private static final int MESSAGE_COMPRESSION_THRESHOLD_SIZE = 102400;
  private final GatewayConnection _connection;
  private static final AMQShortString GZIP_ENCODING =
      AMQShortString.valueOf(GZIPUtils.GZIP_CONTENT_ENCODING);

  private static final Logger LOGGER =
      LoggerFactory.getLogger(org.apache.qpid.server.protocol.v0_8.ProtocolOutputConverter.class);

  public ProtocolOutputConverter(GatewayConnection connection) {
    _connection = connection;
  }

  public long writeDeliver(
      final MessagePublishInfo info,
      final ContentBody contentBody,
      final ContentHeaderBody contentHeaderBody,
      final boolean isRedelivered,
      int channelId,
      long deliveryTag,
      AMQShortString consumerTag) {
    AMQBody deliverBody = createEncodedDeliverBody(info, isRedelivered, deliveryTag, consumerTag);
    return writeMessageDelivery(
        contentBody.getPayload(), contentHeaderBody, channelId, deliverBody);
  }

  interface DisposableMessageContentSource extends MessageContentSource {
    void dispose();
  }

  private long writeMessageDelivery(
      QpidByteBuffer payload,
      ContentHeaderBody contentHeaderBody,
      int channelId,
      AMQBody deliverBody) {

    int bodySize = (int) contentHeaderBody.getBodySize();
    boolean msgCompressed = isCompressed(contentHeaderBody);
    DisposableMessageContentSource modifiedContent = null;

    boolean compressionSupported = _connection.isCompressionSupported();

    long length;
    if (msgCompressed
        && !compressionSupported
        && (modifiedContent = inflateIfPossible(payload)) != null) {
      BasicContentHeaderProperties modifiedProps =
          new BasicContentHeaderProperties(contentHeaderBody.getProperties());
      modifiedProps.setEncoding((String) null);

      length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
    } else if (!msgCompressed
        && compressionSupported
        && contentHeaderBody.getProperties().getEncoding() == null
        && bodySize > MESSAGE_COMPRESSION_THRESHOLD_SIZE
        && (modifiedContent = deflateIfPossible(payload)) != null) {
      BasicContentHeaderProperties modifiedProps =
          new BasicContentHeaderProperties(contentHeaderBody.getProperties());
      modifiedProps.setEncoding(GZIP_ENCODING);

      length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
    } else {
      writeMessageDeliveryUnchanged(
          new ModifiedContentSource(payload), channelId, deliverBody, contentHeaderBody, bodySize);

      length = bodySize;
    }

    if (modifiedContent != null) {
      modifiedContent.dispose();
    }

    return length;
  }

  private DisposableMessageContentSource deflateIfPossible(QpidByteBuffer contentBuffers) {
    try {
      return new ModifiedContentSource(QpidByteBuffer.deflate(contentBuffers));
    } catch (IOException e) {
      LOGGER.warn(
          "Unable to compress message payload for consumer with gzip, message will be sent as is",
          e);
      return null;
    }
  }

  private DisposableMessageContentSource inflateIfPossible(QpidByteBuffer contentBuffers) {
    try {
      return new ModifiedContentSource(QpidByteBuffer.inflate(contentBuffers));
    } catch (IOException e) {
      LOGGER.warn(
          "Unable to decompress message payload for consumer with gzip, message will be sent as is",
          e);
      return null;
    }
  }

  private int writeMessageDeliveryModified(
      final MessageContentSource content,
      final int channelId,
      final AMQBody deliverBody,
      final BasicContentHeaderProperties modifiedProps) {
    final int bodySize = (int) content.getSize();
    ContentHeaderBody modifiedHeaderBody = new ContentHeaderBody(modifiedProps, bodySize);
    writeMessageDeliveryUnchanged(content, channelId, deliverBody, modifiedHeaderBody, bodySize);
    return bodySize;
  }

  private void writeMessageDeliveryUnchanged(
      final MessageContentSource content,
      int channelId,
      AMQBody deliverBody,
      ContentHeaderBody contentHeaderBody,
      int bodySize) {
    if (bodySize == 0) {
      SmallCompositeAMQBodyBlock compositeBlock =
          new SmallCompositeAMQBodyBlock(channelId, deliverBody, contentHeaderBody);

      writeFrame(compositeBlock);
    } else {
      int maxFrameBodySize = (int) _connection.getMaxFrameSize() - AMQFrame.getFrameOverhead();
      try (QpidByteBuffer contentByteBuffer = content.getContent()) {
        int contentChunkSize = bodySize > maxFrameBodySize ? maxFrameBodySize : bodySize;
        QpidByteBuffer body = contentByteBuffer.view(0, contentChunkSize);
        writeFrame(
            new CompositeAMQBodyBlock(
                channelId, deliverBody, contentHeaderBody, new MessageContentSourceBody(body)));

        int writtenSize = contentChunkSize;
        while (writtenSize < bodySize) {
          contentChunkSize =
              (bodySize - writtenSize) > maxFrameBodySize
                  ? maxFrameBodySize
                  : bodySize - writtenSize;
          QpidByteBuffer chunk = contentByteBuffer.view(writtenSize, contentChunkSize);
          writtenSize += contentChunkSize;
          writeFrame(new AMQFrame(channelId, new MessageContentSourceBody(chunk)));
        }
      }
    }
  }

  private boolean isCompressed(final ContentHeaderBody contentHeaderBody) {
    return GZIP_ENCODING.equals(contentHeaderBody.getProperties().getEncoding());
  }

  private static class MessageContentSourceBody implements AMQBody {
    public static final byte TYPE = 3;
    private final int _length;
    private final QpidByteBuffer _content;

    private MessageContentSourceBody(QpidByteBuffer content) {
      _content = content;
      _length = content.remaining();
    }

    @Override
    public byte getFrameType() {
      return TYPE;
    }

    @Override
    public int getSize() {
      return _length;
    }

    @Override
    public long writePayload(final ByteBufferSender sender) {
      try {
        sender.send(_content);
      } finally {
        _content.dispose();
      }
      return _length;
    }

    @Override
    public void handle(int channelId, AMQVersionAwareProtocolSession amqProtocolSession)
        throws QpidException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "[" + getClass().getSimpleName() + ", length: " + _length + "]";
    }
  }

  public long writeGetOk(
      final MessagePublishInfo info,
      final ContentBody contentBody,
      final ContentHeaderBody contentHeaderBody,
      final boolean isRedelivered,
      int channelId,
      long deliveryTag,
      int queueSize) {
    AMQBody deliver = createEncodedGetOkBody(info, isRedelivered, deliveryTag, queueSize);
    return writeMessageDelivery(contentBody.getPayload(), contentHeaderBody, channelId, deliver);
  }

  private AMQBody createEncodedDeliverBody(
      MessagePublishInfo info,
      boolean isRedelivered,
      final long deliveryTag,
      final AMQShortString consumerTag) {

    final AMQShortString exchangeName;
    final AMQShortString routingKey;

    exchangeName = info.getExchange();
    routingKey = info.getRoutingKey();

    return new EncodedDeliveryBody(
        deliveryTag, routingKey, exchangeName, consumerTag, isRedelivered);
  }

  public class EncodedDeliveryBody implements AMQBody {
    private final long _deliveryTag;
    private final AMQShortString _routingKey;
    private final AMQShortString _exchangeName;
    private final AMQShortString _consumerTag;
    private final boolean _isRedelivered;
    private AMQBody _underlyingBody;

    private EncodedDeliveryBody(
        long deliveryTag,
        AMQShortString routingKey,
        AMQShortString exchangeName,
        AMQShortString consumerTag,
        boolean isRedelivered) {
      _deliveryTag = deliveryTag;
      _routingKey = routingKey;
      _exchangeName = exchangeName;
      _consumerTag = consumerTag;
      _isRedelivered = isRedelivered;
    }

    public AMQBody createAMQBody() {
      return _connection
          .getMethodRegistry()
          .createBasicDeliverBody(
              _consumerTag, _deliveryTag, _isRedelivered, _exchangeName, _routingKey);
    }

    @Override
    public byte getFrameType() {
      return AMQMethodBody.TYPE;
    }

    @Override
    public int getSize() {
      if (_underlyingBody == null) {
        _underlyingBody = createAMQBody();
      }
      return _underlyingBody.getSize();
    }

    @Override
    public long writePayload(ByteBufferSender sender) {
      if (_underlyingBody == null) {
        _underlyingBody = createAMQBody();
      }
      return _underlyingBody.writePayload(sender);
    }

    @Override
    public void handle(final int channelId, final AMQVersionAwareProtocolSession amqProtocolSession)
        throws QpidException {
      throw new QpidException("This block should never be dispatched!");
    }

    @Override
    public String toString() {
      return "["
          + getClass().getSimpleName()
          + " underlyingBody: "
          + String.valueOf(_underlyingBody)
          + "]";
    }
  }

  private AMQBody createEncodedGetOkBody(
      MessagePublishInfo info, boolean isRedelivered, long deliveryTag, int queueSize) {
    final AMQShortString exchangeName;
    final AMQShortString routingKey;

    exchangeName = info.getExchange();
    routingKey = info.getRoutingKey();

    return _connection
        .getMethodRegistry()
        .createBasicGetOkBody(deliveryTag, isRedelivered, exchangeName, routingKey, queueSize);
  }

  private AMQBody createEncodedReturnFrame(
      MessagePublishInfo messagePublishInfo, int replyCode, AMQShortString replyText) {

    return _connection
        .getMethodRegistry()
        .createBasicReturnBody(
            replyCode,
            replyText,
            messagePublishInfo.getExchange(),
            messagePublishInfo.getRoutingKey());
  }

  public void writeReturn(
      MessagePublishInfo messagePublishInfo,
      ContentHeaderBody header,
      QpidByteBuffer payload,
      int channelId,
      int replyCode,
      AMQShortString replyText) {
    AMQBody returnFrame = createEncodedReturnFrame(messagePublishInfo, replyCode, replyText);
    writeMessageDelivery(payload, header, channelId, returnFrame);
  }

  public void writeFrame(AMQDataBlock block) {
    _connection.writeFrame(block);
  }

  public void confirmConsumerAutoClose(int channelId, AMQShortString consumerTag) {

    BasicCancelOkBody basicCancelOkBody =
        _connection.getMethodRegistry().createBasicCancelOkBody(consumerTag);
    writeFrame(basicCancelOkBody.generateFrame(channelId));
  }

  public static final class CompositeAMQBodyBlock extends AMQDataBlock {
    public static final int OVERHEAD = 3 * AMQFrame.getFrameOverhead();

    private final AMQBody _methodBody;
    private final AMQBody _headerBody;
    private final AMQBody _contentBody;
    private final int _channel;

    public CompositeAMQBodyBlock(
        int channel, AMQBody methodBody, AMQBody headerBody, AMQBody contentBody) {
      _channel = channel;
      _methodBody = methodBody;
      _headerBody = headerBody;
      _contentBody = contentBody;
    }

    @Override
    public long getSize() {
      return OVERHEAD
          + (long) _methodBody.getSize()
          + (long) _headerBody.getSize()
          + (long) _contentBody.getSize();
    }

    @Override
    public long writePayload(final ByteBufferSender sender) {
      long size = (new AMQFrame(_channel, _methodBody)).writePayload(sender);

      size += (new AMQFrame(_channel, _headerBody)).writePayload(sender);

      size += (new AMQFrame(_channel, _contentBody)).writePayload(sender);

      return size;
    }

    @VisibleForTesting
    public AMQBody getMethodBody() {
      return _methodBody;
    }

    @VisibleForTesting
    public AMQBody getHeaderBody() {
      return _headerBody;
    }

    @VisibleForTesting
    public AMQBody getContentBody() {
      return _contentBody;
    }

    @VisibleForTesting
    public int getChannel() {
      return _channel;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder
          .append("[")
          .append(getClass().getSimpleName())
          .append(" methodBody=")
          .append(_methodBody)
          .append(", headerBody=")
          .append(_headerBody)
          .append(", contentBody=")
          .append(_contentBody)
          .append(", channel=")
          .append(_channel)
          .append("]");
      return builder.toString();
    }
  }

  public static final class SmallCompositeAMQBodyBlock extends AMQDataBlock {
    public static final int OVERHEAD = 2 * AMQFrame.getFrameOverhead();

    private final AMQBody _methodBody;
    private final AMQBody _headerBody;
    private final int _channel;

    public SmallCompositeAMQBodyBlock(int channel, AMQBody methodBody, AMQBody headerBody) {
      _channel = channel;
      _methodBody = methodBody;
      _headerBody = headerBody;
    }

    @Override
    public long getSize() {
      return OVERHEAD + (long) _methodBody.getSize() + (long) _headerBody.getSize();
    }

    @Override
    public long writePayload(final ByteBufferSender sender) {
      long size = (new AMQFrame(_channel, _methodBody)).writePayload(sender);
      size += (new AMQFrame(_channel, _headerBody)).writePayload(sender);
      return size;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder
          .append(getClass().getSimpleName())
          .append("methodBody=")
          .append(_methodBody)
          .append(", headerBody=")
          .append(_headerBody)
          .append(", channel=")
          .append(_channel)
          .append("]");
      return builder.toString();
    }
  }

  private static class ModifiedContentSource implements DisposableMessageContentSource {
    private final QpidByteBuffer _buffer;
    private final int _size;

    public ModifiedContentSource(final QpidByteBuffer buffer) {
      _buffer = buffer;
      _size = _buffer.remaining();
    }

    @Override
    public void dispose() {
      _buffer.dispose();
    }

    @Override
    public QpidByteBuffer getContent() {
      return getContent(0, (int) getSize());
    }

    @Override
    public QpidByteBuffer getContent(final int offset, int length) {
      return _buffer.view(offset, length);
    }

    @Override
    public long getSize() {
      return _size;
    }
  }
}
