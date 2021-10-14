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

import java.util.Collections;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

@Getter
@Setter
public class GatewayConfiguration extends ProxyConfiguration {
  private static final String CATEGORY_AMQP = "AMQP";

  @FieldContext(
    category = CATEGORY_AMQP,
    required = true,
    doc =
        "Used to specify multiple advertised listeners for the gateway."
            + " The value must format as amqp[s]://<host>:<port>,"
            + "multiple listeners should be separated with commas."
  )
  private Set<String> amqpListeners = Collections.singleton("amqp://127.0.0.1:5672");

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "Authentication mechanism name list for AMQP (a comma-separated list of mecanisms. Eg: PLAIN,EXTERNAL)"
  )
  private Set<String> amqpAuthenticationMechanisms = Collections.singleton("PLAIN");

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "If set, the RabbitMQ service will use these parameters to authenticate on Pulsar's brokers. If not set, the brokerClientAuthenticationParameters setting will be used. This setting allows to have different credentials for the proxy and for the RabbitMQ service"
  )
  private String amqpBrokerClientAuthenticationParameters;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc = "The maximum number of sessions which can exist concurrently on an AMQP connection."
  )
  private int amqpSessionCountLimit = 256;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "The default period with which Broker and client will exchange"
            + " heartbeat messages (in seconds) when using AMQP. Clients may negotiate a different heartbeat"
            + " frequency or disable it altogether."
  )
  private int amqpHeartbeatDelay = 0;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "Factor to determine the maximum length of that may elapse between heartbeats being"
            + " received from the peer before an AMQP0.9 connection is deemed to have been broken."
  )
  private int amqpHeartbeatTimeoutFactor = 2;

  @FieldContext(category = CATEGORY_AMQP, doc = "AMQP Network buffer size.")
  // TODO: Network buffer size must be bigger than Netty's receive buffer. Also configure Netty with
  //  this.
  private int amqpNetworkBufferSize = 2 * 1024 * 1024;

  @FieldContext(category = CATEGORY_AMQP, doc = "AMQP Max message size.")
  private int amqpMaxMessageSize = 100 * 1024 * 1024;

  @FieldContext(category = CATEGORY_AMQP, doc = "AMQP Length of binary data sent to debug log.")
  private int amqpDebugBinaryDataLength = 80;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "Timeout in ms after which the AMQP connection closes even if a ConnectionCloseOk frame is not received"
  )
  private int amqpConnectionCloseTimeout = 2000;

  @FieldContext(category = CATEGORY_AMQP, doc = "Whether batching messages is enabled in AMQP")
  private boolean amqpBatchingEnabled = true;
}
