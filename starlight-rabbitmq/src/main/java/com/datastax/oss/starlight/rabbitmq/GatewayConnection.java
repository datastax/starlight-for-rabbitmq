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

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.datastax.oss.starlight.rabbitmq.metadata.ContextMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.ExchangeMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQDecoder;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.ServerDecoder;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolHeaderException;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodDispatcher;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls the Qpid AMQP {@link ServerDecoder} from Netty's buffers, handles AMQP
 * connection/disconnection frames and heartbeat. Most of the code is adapted from {@link
 * org.apache.qpid.server.protocol.v0_8.AMQPConnection_0_8Impl}.
 */
public class GatewayConnection extends ChannelInboundHandlerAdapter
    implements ServerMethodProcessor<ServerChannelMethodProcessor> {

  enum ConnectionState {
    INIT,
    AWAIT_START_OK,
    AWAIT_SECURE_OK,
    AWAIT_TUNE_OK,
    AWAIT_OPEN,
    OPEN
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayConnection.class);

  public static final RetryPolicy<Object> ZK_CONFLICT_RETRY =
      new RetryPolicy<>()
          .handle(KeeperException.BadVersionException.class)
          .onRetriesExceeded(lis -> LOGGER.error("Zookeeper Retries exceeded", lis.getFailure()))
          .withJitter(0.3)
          .withDelay(Duration.ofMillis(50))
          .withMaxRetries(100);

  private final GatewayService gatewayService;
  private ChannelHandlerContext ctx;
  private SocketAddress remoteAddress;
  private String namespace;
  private String role;

  // Variables copied from Qpid's AMQPConnection_0_8Impl
  private ServerDecoder _decoder;
  private volatile int _maxNoOfChannels;
  private ProtocolVersion _protocolVersion;
  private volatile MethodRegistry _methodRegistry;
  private volatile ConnectionState _state = ConnectionState.INIT;
  private final ConcurrentLongHashMap<AMQChannel> _channelMap = new ConcurrentLongHashMap<>();
  private final ProtocolOutputConverter _protocolOutputConverter;
  private volatile int _maxFrameSize;
  private final AtomicBoolean _orderlyClose = new AtomicBoolean(false);
  private final Map<Integer, Long> _closingChannelsList = new ConcurrentHashMap<>();
  private volatile int _currentClassId;
  private volatile int _currentMethodId;
  private volatile int _heartBeatDelay;

  // Variables copied from Qpid's NonBlockingConnectionPlainDelegate
  private final int _networkBufferSize;
  private volatile QpidByteBuffer _netInputBuffer;

  public GatewayConnection(GatewayService gatewayService) {
    this.gatewayService = gatewayService;
    this._networkBufferSize = gatewayService.getConfig().getAmqpNetworkBufferSize();
    this._protocolOutputConverter = new ProtocolOutputConverter(this);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    this.remoteAddress = ctx.channel().remoteAddress();
    this.ctx = ctx;
    this._decoder = new ServerDecoder(this);
    this._netInputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    completeAndCloseAllChannels();
    this._netInputBuffer = null;
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
    }
    if (ctx.channel().isWritable()) {
      _channelMap.values().forEach(AMQChannel::startConsumers);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOGGER.warn(
        "[{}] Got exception {} : {} {}",
        ctx,
        cause.getClass().getSimpleName(),
        cause.getMessage(),
        cause);
    closeNetworkConnection();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buffer = (ByteBuf) msg;
    gatewayService.incrementBytesIn(namespace, buffer.readableBytes());
    try {
      QpidByteBuffer buf = QpidByteBuffer.wrap(buffer.nioBuffer());
      if (_netInputBuffer.remaining() < buf.remaining()) {
        QpidByteBuffer oldBuffer = _netInputBuffer;
        _netInputBuffer = QpidByteBuffer.allocateDirect(_networkBufferSize);
        if (oldBuffer.position() != 0) {
          oldBuffer.limit(oldBuffer.position());
          oldBuffer.slice();
          oldBuffer.flip();
          _netInputBuffer.put(oldBuffer);
        }
      }
      _netInputBuffer.put(buf);
      _netInputBuffer.flip();
      _decoder.decodeBuffer(_netInputBuffer);
      receivedCompleteAllChannels();
      if (_netInputBuffer != null) {
        restoreApplicationBufferForWrite();
      }
    } catch (AMQFrameDecodingException e) {
      LOGGER.debug("Invalid frame", e);
      sendConnectionClose(
          // To be compliant with RabbitMQ expectations
          e.getMessage().startsWith("Unsupported content header class id:")
              ? 505
              : e.getErrorCode(),
          e.getMessage(),
          0);
    } catch (Exception e) {
      LOGGER.warn("Unexpected exception", e);
      closeNetworkConnection();
    } finally {
      buffer.release();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        closeNetworkConnection();
      } else if (e.state() == IdleState.WRITER_IDLE) {
        ctx.writeAndFlush(HeartbeatBody.FRAME);
      }
    }
  }

  /** See {@link org.apache.qpid.server.transport.NonBlockingConnectionPlainDelegate} */
  private void restoreApplicationBufferForWrite() {
    try (QpidByteBuffer oldNetInputBuffer = _netInputBuffer) {
      int unprocessedDataLength = _netInputBuffer.remaining();
      _netInputBuffer.limit(_netInputBuffer.capacity());
      _netInputBuffer = oldNetInputBuffer.slice();
      _netInputBuffer.limit(unprocessedDataLength);
    }
    if (_netInputBuffer.limit() != _netInputBuffer.capacity()) {
      _netInputBuffer.position(_netInputBuffer.limit());
      _netInputBuffer.limit(_netInputBuffer.capacity());
    } else {
      try (QpidByteBuffer currentBuffer = _netInputBuffer) {
        int newBufSize;

        if (currentBuffer.capacity() < _networkBufferSize) {
          newBufSize = _networkBufferSize;
        } else {
          newBufSize = currentBuffer.capacity() + _networkBufferSize;
          // TODO: reportUnexpectedByteBufferSizeUsage
          // _parent.reportUnexpectedByteBufferSizeUsage();
        }

        _netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
        _netInputBuffer.put(currentBuffer);
      }
    }
  }

  @Override
  public void receiveConnectionStartOk(
      FieldTable clientProperties,
      AMQShortString mechanism,
      byte[] response,
      AMQShortString locale) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV ConnectionStartOk["
              + " clientProperties: "
              + clientProperties
              + " mechanism: "
              + mechanism
              + " response: ********"
              + " locale: "
              + locale
              + " ]");
    }

    assertState(ConnectionState.AWAIT_START_OK);

    LOGGER.debug("SASL Mechanism selected: {} Locale : {}", mechanism, locale);

    if (mechanism == null || mechanism.length() == 0) {
      sendConnectionClose(ErrorCodes.CONNECTION_FORCED, "No Sasl mechanism was specified", 0);
      return;
    }

    if (gatewayService.getConfig().isAuthenticationEnabled()) {
      role = null;
      AuthenticationService authenticationService = gatewayService.getAuthenticationService();
      try {
        if ("PLAIN".equals(mechanism.toString())) {
          if (authenticationService.getAuthenticationProvider("token") == null) {
            throw new AuthenticationException("SASL PLAIN mechanism not enabled");
          }
          if (response.length > 2 && response[0] == 0 && response[1] == 0) {
            String token = new String(response, 2, response.length - 2, StandardCharsets.UTF_8);
            AuthenticationDataCommand authData =
                new AuthenticationDataCommand(token, remoteAddress, null);
            role = authenticationService.authenticate(authData, "token");
          }
          if (role == null) {
            throw new AuthenticationException(
                "SASL PLAIN is only supported with JWT as password at the moment");
          }
        } else if ("EXTERNAL".equals(mechanism.toString())) {
          if (authenticationService.getAuthenticationProvider("tls") == null) {
            throw new AuthenticationException("SASL EXTERNAL mechanism not enabled");
          }
          ChannelHandler sslHandler =
              ctx.channel().pipeline().get(ServiceChannelInitializer.TLS_HANDLER);
          SSLSession sslSession = null;
          if (sslHandler != null) {
            sslSession = ((SslHandler) sslHandler).engine().getSession();
          }
          AuthenticationDataCommand authData =
              new AuthenticationDataCommand(null, remoteAddress, sslSession);
          role = authenticationService.authenticate(authData, "tls");
        } else {
          throw new AuthenticationException("Unsupported authentication mechanism");
        }
      } catch (AuthenticationException e) {
        LOGGER.debug("Invalid authentication", e);
        sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Invalid authentication", 0);
        return;
      }
    }
    // setClientProperties(clientProperties);
    processSaslResponse(response);

    _state = ConnectionState.AWAIT_TUNE_OK;
  }

  @Override
  public void receiveConnectionSecureOk(byte[] response) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV ConnectionSecureOk[ response: ******** ] ");
    }

    assertState(ConnectionState.AWAIT_SECURE_OK);

    processSaslResponse(response);
  }

  private void processSaslResponse(final byte[] response) {
    int frameMax = getDefaultMaxFrameSize();

    if (frameMax <= 0) {
      frameMax = Integer.MAX_VALUE;
    }

    ConnectionTuneBody tuneBody =
        _methodRegistry.createConnectionTuneBody(
            gatewayService.getConfig().getAmqpSessionCountLimit(),
            frameMax,
            gatewayService.getConfig().getAmqpHeartbeatDelay());
    writeFrame(tuneBody.generateFrame(0));
  }

  @Override
  public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV ConnectionTuneOk["
              + " channelMax: "
              + channelMax
              + " frameMax: "
              + frameMax
              + " heartbeat: "
              + heartbeat
              + " ]");
    }

    assertState(ConnectionState.AWAIT_TUNE_OK);

    if (heartbeat > 0) {
      _heartBeatDelay = heartbeat;
      ctx.channel()
          .pipeline()
          .addFirst(
              "idleStateHandler",
              new IdleStateHandler(
                  heartbeat * gatewayService.getConfig().getAmqpHeartbeatTimeoutFactor(),
                  heartbeat,
                  0));
    }

    int brokerFrameMax = getDefaultMaxFrameSize();
    if (brokerFrameMax <= 0) {
      brokerFrameMax = Integer.MAX_VALUE;
    }

    if (frameMax > (long) brokerFrameMax) {
      sendConnectionClose(
          ErrorCodes.SYNTAX_ERROR,
          "Attempt to set max frame size to "
              + frameMax
              + " greater than the broker will allow: "
              + brokerFrameMax,
          0);
    } else if (frameMax > 0 && frameMax < AMQDecoder.FRAME_MIN_SIZE) {
      sendConnectionClose(
          ErrorCodes.SYNTAX_ERROR,
          "Attempt to set max frame size to "
              + frameMax
              + " which is smaller than the specification defined minimum: "
              + AMQDecoder.FRAME_MIN_SIZE,
          0);
    } else {
      int calculatedFrameMax = frameMax == 0 ? brokerFrameMax : (int) frameMax;
      setMaxFrameSize(calculatedFrameMax);

      // 0 means no implied limit, except that forced by protocol limitations (0xFFFF)
      _maxNoOfChannels = ((channelMax == 0) || (channelMax > 0xFFFF)) ? 0xFFFF : channelMax;
    }
    _state = ConnectionState.AWAIT_OPEN;
  }

  public void setMaxFrameSize(int frameMax) {
    _maxFrameSize = frameMax;
    _decoder.setMaxFrameSize(frameMax);
  }

  public long getMaxFrameSize() {
    return _maxFrameSize;
  }

  private int getDefaultMaxFrameSize() {
    // QPID-6784 : Some old clients send payload with size equals to max frame size
    // we want to fit those frames into the network buffer
    return gatewayService.getConfig().getAmqpNetworkBufferSize() - AMQFrame.getFrameOverhead();
  }

  public int getSessionCountLimit() {
    return _maxNoOfChannels;
  }

  public int getHeartbeatDelay() {
    return _heartBeatDelay;
  }

  void assertState(final ConnectionState requiredState) {
    if (_state != requiredState) {
      String replyText = "Command Invalid, expected " + requiredState + " but was " + _state;
      sendConnectionClose(ErrorCodes.COMMAND_INVALID, replyText, 0);
      throw new ConnectionScopedRuntimeException(replyText);
    }
  }

  @Override
  public void receiveConnectionOpen(
      AMQShortString virtualHostName, AMQShortString capabilities, boolean insist) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV ConnectionOpen["
              + " virtualHost: "
              + virtualHostName
              + " capabilities: "
              + capabilities
              + " insist: "
              + insist
              + " ]");
    }

    assertState(ConnectionState.AWAIT_OPEN);

    String virtualHostStr = AMQShortString.toString(virtualHostName);
    String tenant = "public";
    if ((virtualHostStr != null)) {
      if (virtualHostStr.charAt(0) == '/') {
        virtualHostStr = virtualHostStr.substring(1);
      }
      int index = virtualHostStr.indexOf("/");
      if (index != -1 && index != virtualHostStr.length() - 1) {
        tenant = virtualHostStr.substring(0, index);
        virtualHostStr = virtualHostStr.substring(index + 1);
      }
    }

    try {
      NamespaceName namespaceName =
          NamespaceName.get(
              tenant, StringUtils.isEmpty(virtualHostStr) ? "default" : virtualHostStr);
      this.namespace = namespaceName.toString();

      if (getGatewayService().getConfig().isAuthorizationEnabled()) {
        CompletableFuture<Boolean> isTenantAdmin =
            getGatewayService()
                .getPulsarAdmin()
                .tenants()
                .getTenantInfoAsync(namespaceName.getTenant())
                .thenApply(tenantInfo -> tenantInfo.getAdminRoles().contains(role));

        CompletableFuture<Map<String, Set<AuthAction>>> namespacePermissions =
            getGatewayService().getPulsarAdmin().namespaces().getPermissionsAsync(namespace);

        namespacePermissions
            .thenAcceptBoth(
                isTenantAdmin,
                (__, isAdmin) -> {
                  if (!isAdmin) {
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug(
                          "Authorization failed: user {} is not tenant admin of {}",
                          role,
                          namespace);
                    }
                    sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Authorization failed", 0);
                  } else {
                    connectionOpen(virtualHostName);
                  }
                })
            .exceptionally(
                t -> {
                  if (t.getCause() instanceof PulsarAdminException.NotFoundException) {
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug("Authorization failed: namespace {} not found", namespace);
                    }
                  } else {
                    LOGGER.error("Authorization failed: Failed to get tenant or namespace", t);
                  }
                  sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Authorization failed", 0);
                  return null;
                });
      } else {
        connectionOpen(virtualHostName);
      }

      // For now, just checking that the namespace exists
    } catch (IllegalArgumentException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Invalid virtual host :" + virtualHostName, e);
      }
      sendConnectionClose(ErrorCodes.INVALID_PATH, e.getMessage(), 0);
    }
  }

  private void connectionOpen(AMQShortString virtualHostName) {
    Failsafe.with(ZK_CONFLICT_RETRY)
        .getStageAsync(
            () -> {
              Versioned<ContextMetadata> versionedContext =
                  getGatewayService().getContextMetadata();
              if (!versionedContext.model().getVhosts().containsKey(namespace)) {
                Versioned<ContextMetadata> newContext =
                    getGatewayService().newContextMetadata(versionedContext);
                VirtualHostMetadata virtualHostMetadata = new VirtualHostMetadata();
                virtualHostMetadata.setNamespace(namespace);
                virtualHostMetadata
                    .getExchanges()
                    .put(
                        ExchangeDefaults.DEFAULT_EXCHANGE_NAME,
                        new ExchangeMetadata(
                            ExchangeMetadata.Type.direct, true, LifetimePolicy.PERMANENT));
                virtualHostMetadata
                    .getExchanges()
                    .put(
                        ExchangeDefaults.DIRECT_EXCHANGE_NAME,
                        new ExchangeMetadata(
                            ExchangeMetadata.Type.direct, true, LifetimePolicy.PERMANENT));
                virtualHostMetadata
                    .getExchanges()
                    .put(
                        ExchangeDefaults.FANOUT_EXCHANGE_NAME,
                        new ExchangeMetadata(
                            ExchangeMetadata.Type.fanout, true, LifetimePolicy.PERMANENT));
                virtualHostMetadata
                    .getExchanges()
                    .put(
                        ExchangeDefaults.TOPIC_EXCHANGE_NAME,
                        new ExchangeMetadata(
                            ExchangeMetadata.Type.topic, true, LifetimePolicy.PERMANENT));
                virtualHostMetadata
                    .getExchanges()
                    .put(
                        ExchangeDefaults.HEADERS_EXCHANGE_NAME,
                        new ExchangeMetadata(
                            ExchangeMetadata.Type.headers, true, LifetimePolicy.PERMANENT));
                newContext.model().getVhosts().put(namespace, virtualHostMetadata);
                return getGatewayService().saveContext(newContext);
              } else {
                return CompletableFuture.completedFuture(null);
              }
            })
        .thenAccept(
            it -> {
              AMQMethodBody responseBody =
                  getMethodRegistry().createConnectionOpenOkBody(virtualHostName);
              writeFrame(responseBody.generateFrame(0));
              _state = ConnectionState.OPEN;
            })
        .exceptionally(
            t -> {
              String errorMessage =
                  "Error while saving new vhost in configuration store: '" + namespace + "'";
              LOGGER.error(errorMessage, t);
              sendConnectionClose(ErrorCodes.INTERNAL_ERROR, errorMessage, 0);
              return null;
            });
  }

  @Override
  public void receiveChannelOpen(int channelId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV[" + channelId + "] ChannelOpen");
    }
    assertState(ConnectionState.OPEN);

    if (namespace == null) {
      sendConnectionClose(
          ErrorCodes.COMMAND_INVALID,
          "Virtualhost has not yet been set. ConnectionOpen has not been called.",
          channelId);
    } else if (getChannel(channelId) != null || channelAwaitingClosure(channelId)) {
      sendConnectionClose(
          ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " already exists", channelId);
    } else if (channelId > getSessionCountLimit()) {
      sendConnectionClose(
          ErrorCodes.CHANNEL_ERROR,
          "Channel "
              + channelId
              + " cannot be created as the max allowed channel id is "
              + getSessionCountLimit(),
          channelId);
    } else {
      LOGGER.debug("Connecting to: {}", namespace);
      final AMQChannel channel = new AMQChannel(this, channelId);
      addChannel(channel);
      ChannelOpenOkBody response;
      response = getMethodRegistry().createChannelOpenOkBody();
      writeFrame(response.generateFrame(channelId));
    }
  }

  @Override
  public ProtocolVersion getProtocolVersion() {
    return _protocolVersion;
  }

  public void setProtocolVersion(ProtocolVersion pv) {
    this._protocolVersion = pv;
    this._methodRegistry = new MethodRegistry(_protocolVersion);
  }

  @Override
  public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
    assertState(ConnectionState.OPEN);

    ServerChannelMethodProcessor channelMethodProcessor = getChannel(channelId);
    if (channelMethodProcessor == null) {
      channelMethodProcessor =
          (ServerChannelMethodProcessor)
              Proxy.newProxyInstance(
                  ServerMethodDispatcher.class.getClassLoader(),
                  new Class[] {ServerChannelMethodProcessor.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("receiveChannelCloseOk")
                        && channelAwaitingClosure(channelId)) {
                      closeChannelOk(channelId);
                    } else if (method.getName().startsWith("receive")) {
                      sendConnectionClose(
                          ErrorCodes.CHANNEL_ERROR, "Unknown channel id: " + channelId, channelId);
                    } else if (method.getName().equals("ignoreAllButCloseOk")) {
                      return channelAwaitingClosure(channelId);
                    }
                    return null;
                  });
    }
    return channelMethodProcessor;
  }

  @Override
  public void receiveConnectionClose(
      int replyCode, AMQShortString replyText, int classId, int methodId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV ConnectionClose["
              + " replyCode: "
              + replyCode
              + " replyText: "
              + replyText
              + " classId: "
              + classId
              + " methodId: "
              + methodId
              + " ]");
    }

    try {
      if (_orderlyClose.compareAndSet(false, true)) {
        completeAndCloseAllChannels();
      }

      MethodRegistry methodRegistry = getMethodRegistry();
      ConnectionCloseOkBody responseBody = methodRegistry.createConnectionCloseOkBody();
      writeFrame(responseBody.generateFrame(0));
    } catch (Exception e) {
      LOGGER.error("Error closing connection for " + remoteAddress, e);
    } finally {
      closeNetworkConnection();
    }
  }

  @Override
  public void receiveConnectionCloseOk() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV ConnectionCloseOk");
    }

    closeNetworkConnection();
  }

  public void closeNetworkConnection() {
    if (!_orderlyClose.get()) {
      completeAndCloseAllChannels();
    }
    ctx.close();
  }

  @Override
  public void receiveHeartbeat() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV Heartbeat");
    }

    // No op
  }

  @Override
  public void receiveProtocolHeader(ProtocolInitiation pi) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV ProtocolHeader [" + pi + " ]");
    }

    // this ensures the codec never checks for a PI message again
    _decoder.setExpectProtocolInitiation(false);
    try {
      ProtocolVersion pv = pi.checkVersion(); // Fails if not correct
      setProtocolVersion(pv);

      String mechanisms =
          String.join(" ", gatewayService.getConfig().getAmqpAuthenticationMechanisms());

      String locales = "en_US";

      // TODO: add recommended connection properties
      Map<String, Object> props = new HashMap<>();
      Map<String, Object> capabilities = new HashMap<>();
      capabilities.put("basic.nack", true);
      capabilities.put("publisher_confirms", true);
      props.put("capabilities", capabilities);

      FieldTable serverProperties = FieldTable.convertToFieldTable(props);

      AMQMethodBody responseBody =
          getMethodRegistry()
              .createConnectionStartBody(
                  pv.getMajorVersion(),
                  pv.getActualMinorVersion(),
                  serverProperties,
                  mechanisms.getBytes(US_ASCII),
                  locales.getBytes(US_ASCII));
      writeFrame(responseBody.generateFrame(0));
      _state = ConnectionState.AWAIT_START_OK;
    } catch (QpidException | AMQProtocolHeaderException e) {
      LOGGER.debug(
          "Received unsupported protocol initiation for protocol version: {} ",
          getProtocolVersion(),
          e);

      writeFrame(new ProtocolInitiation(ProtocolVersion.getLatestSupportedVersion()));
      closeNetworkConnection();
    }
  }

  @Override
  public void setCurrentMethod(int classId, int methodId) {
    _currentClassId = classId;
    _currentMethodId = methodId;
  }

  public boolean isClosing() {
    return _orderlyClose.get();
  }

  @Override
  public boolean ignoreAllButCloseOk() {
    return isClosing();
  }

  private void receivedCompleteAllChannels() {
    // TODO: see AMQPConnection_0_8Impl::receivedCompleteAllChannels
  }

  public AMQChannel getChannel(int channelId) {
    final AMQChannel channel = _channelMap.get(channelId);
    if ((channel == null) || channel.isClosing()) {
      return null;
    } else {
      return channel;
    }
  }

  public boolean channelAwaitingClosure(int channelId) {
    return ignoreAllButCloseOk()
        || (!_closingChannelsList.isEmpty() && _closingChannelsList.containsKey(channelId));
  }

  private void addChannel(AMQChannel channel) {
    _channelMap.put(channel.getChannelId(), channel);
  }

  private void removeChannel(int channelId) {
    _channelMap.remove(channelId);
  }

  public synchronized void writeFrame(AMQDataBlock frame) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("SEND: " + frame);
    }

    ctx.writeAndFlush(frame)
        .addListener(msg -> gatewayService.incrementBytesOut(namespace, frame.getSize()));
  }

  public void closeChannel(AMQChannel channel) {
    closeChannel(channel, 0, null, false);
  }

  public void closeChannelAndWriteFrame(AMQChannel channel, int cause, String message) {
    writeFrame(
        new AMQFrame(
            channel.getChannelId(),
            getMethodRegistry()
                .createChannelCloseBody(
                    cause,
                    AMQShortString.validValueOf(message),
                    _currentClassId,
                    _currentMethodId)));
    closeChannel(channel, cause, message, true);
  }

  void closeChannel(AMQChannel channel, int cause, String message, boolean mark) {
    int channelId = channel.getChannelId();
    try {
      channel.close(cause, message);
      if (mark) {
        markChannelAwaitingCloseOk(channelId);
      }
    } finally {
      removeChannel(channelId);
    }
  }

  public void closeChannelOk(int channelId) {
    _closingChannelsList.remove(channelId);
  }

  @VisibleForTesting
  void markChannelAwaitingCloseOk(int channelId) {
    _closingChannelsList.put(channelId, System.currentTimeMillis());
  }

  public void sendConnectionClose(int errorCode, String message, int channelId) {
    sendConnectionClose(
        channelId,
        new AMQFrame(
            0,
            new ConnectionCloseBody(
                getProtocolVersion(),
                errorCode,
                AMQShortString.validValueOf(message),
                _currentClassId,
                _currentMethodId)));
  }

  private void completeAndCloseAllChannels() {
    try {
      receivedCompleteAllChannels();
    } finally {
      closeAllChannels();
    }
  }

  private void closeAllChannels() {
    try {
      RuntimeException firstException = null;
      for (AMQChannel channel : _channelMap.values()) {
        try {
          channel.close();
        } catch (RuntimeException re) {
          if (!(re instanceof ConnectionScopedRuntimeException)) {
            LOGGER.error("Unexpected exception closing channel", re);
          }
          firstException = re;
        }
      }

      if (firstException != null) {
        throw firstException;
      }
    } finally {
      _channelMap.clear();
    }
  }

  private void sendConnectionClose(int channelId, AMQFrame frame) {
    if (_orderlyClose.compareAndSet(false, true)) {
      try {
        markChannelAwaitingCloseOk(channelId);
        completeAndCloseAllChannels();
      } finally {
        try {
          writeFrame(frame);
        } finally {
          ctx.executor()
              .schedule(
                  this::closeNetworkConnection,
                  gatewayService.getConfig().getAmqpConnectionCloseTimeout(),
                  TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  // TODO: support message compression (Qpid only)
  public boolean isCompressionSupported() {
    return false;
  }

  public MethodRegistry getMethodRegistry() {
    return _methodRegistry;
  }

  public ProtocolOutputConverter getProtocolOutputConverter() {
    return _protocolOutputConverter;
  }

  public String getNamespace() {
    return namespace;
  }

  public GatewayService getGatewayService() {
    return gatewayService;
  }

  public ChannelHandlerContext getCtx() {
    return ctx;
  }
}
