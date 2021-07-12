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

import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

@Getter
@Setter
public class GatewayConfiguration implements PulsarConfiguration {
  @Category private static final String CATEGORY_SERVER = "Server";
  @Category private static final String CATEGORY_BROKER_DISCOVERY = "Broker Discovery";

  @Category
  private static final String CATEGORY_CLIENT_AUTHENTICATION = "Broker Client Authorization";

  @Category private static final String CATEGORY_TLS = "TLS";
  @Category private static final String CATEGORY_KEYSTORE_TLS = "KeyStoreTLS";

  private static final String CATEGORY_AMQP = "AMQP";

  @FieldContext(
    category = CATEGORY_BROKER_DISCOVERY,
    doc = "The service url points to the broker cluster"
  )
  private String brokerServiceURL;

  @FieldContext(
    category = CATEGORY_BROKER_DISCOVERY,
    doc = "The web service url points to the broker cluster"
  )
  private String brokerWebServiceURL;

  @FieldContext(category = CATEGORY_SERVER, doc = "Hostname or IP address the service binds on")
  private String bindAddress = "0.0.0.0";

  @FieldContext(
    category = CATEGORY_SERVER,
    doc =
        "Hostname or IP address the service advertises to the outside world."
            + " If not set, the value of `InetAddress.getLocalHost().getCanonicalHostName()` is used."
  )
  private String advertisedAddress;

  @FieldContext(category = CATEGORY_SERVER, doc = "The port for serving AMQP")
  private Optional<Integer> servicePort = Optional.of(5672);

  @FieldContext(category = CATEGORY_SERVER, doc = "The port for serving tls secured AMQP")
  private Optional<Integer> servicePortTls = Optional.empty();

  @FieldContext(
    category = CATEGORY_CLIENT_AUTHENTICATION,
    doc = "The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers"
  )
  private String brokerClientAuthenticationPlugin;

  @FieldContext(
    category = CATEGORY_CLIENT_AUTHENTICATION,
    doc =
        "The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers"
  )
  private String brokerClientAuthenticationParameters;

  @FieldContext(
    category = CATEGORY_CLIENT_AUTHENTICATION,
    doc =
        "The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers"
  )
  private String brokerClientTrustCertsFilePath;

  /** *** --- TLS --- *** */
  @Deprecated private boolean tlsEnabledInProxy = false;

  @FieldContext(
    category = CATEGORY_TLS,
    doc = "Tls cert refresh duration in seconds (set 0 to check on every new connection)"
  )
  private long tlsCertRefreshCheckDurationSec = 300; // 5 mins

  @FieldContext(category = CATEGORY_TLS, doc = "Path for the TLS certificate file")
  private String tlsCertificateFilePath;

  @FieldContext(category = CATEGORY_TLS, doc = "Path for the TLS private key file")
  private String tlsKeyFilePath;

  @FieldContext(
    category = CATEGORY_TLS,
    doc =
        "Path for the trusted TLS certificate file.\n\n"
            + "This cert is used to verify that any certs presented by connecting clients"
            + " are signed by a certificate authority. If this verification fails, then the"
            + " certs are untrusted and the connections are dropped"
  )
  private String tlsTrustCertsFilePath;

  @FieldContext(
    category = CATEGORY_TLS,
    doc =
        "Accept untrusted TLS certificate from client.\n\n"
            + "If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath`"
            + " cert will be allowed to connect to the server, though the cert will not be used for"
            + " client authentication"
  )
  private boolean tlsAllowInsecureConnection = false;

  @FieldContext(
    category = CATEGORY_TLS,
    doc = "Whether the hostname is validated when the proxy creates a TLS connection with brokers"
  )
  private boolean tlsHostnameVerificationEnabled = false;

  @FieldContext(
    category = CATEGORY_TLS,
    doc =
        "Specify the tls protocols the broker will use to negotiate during TLS handshake"
            + " (a comma-separated list of protocol names).\n\n"
            + "Examples:- [TLSv1.3, TLSv1.2]"
  )
  private Set<String> tlsProtocols = Sets.newTreeSet();

  @FieldContext(
    category = CATEGORY_TLS,
    doc =
        "Specify the tls cipher the proxy will use to negotiate during TLS Handshake"
            + " (a comma-separated list of ciphers).\n\n"
            + "Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
  )
  private Set<String> tlsCiphers = Sets.newTreeSet();

  @FieldContext(
    category = CATEGORY_TLS,
    doc =
        "Whether client certificates are required for TLS.\n\n"
            + " Connections are rejected if the client certificate isn't trusted"
  )
  private boolean tlsRequireTrustedClientCertOnConnect = false;

  /** ** --- KeyStore TLS config variables --- *** */
  @FieldContext(
    category = CATEGORY_KEYSTORE_TLS,
    doc = "Enable TLS with KeyStore type configuration for proxy"
  )
  private boolean tlsEnabledWithKeyStore = false;

  @FieldContext(category = CATEGORY_KEYSTORE_TLS, doc = "TLS Provider")
  private String tlsProvider = null;

  @FieldContext(
    category = CATEGORY_KEYSTORE_TLS,
    doc = "TLS KeyStore type configuration for proxy: JKS, PKCS12"
  )
  private String tlsKeyStoreType = "JKS";

  @FieldContext(category = CATEGORY_KEYSTORE_TLS, doc = "TLS KeyStore path for proxy")
  private String tlsKeyStore = null;

  @FieldContext(category = CATEGORY_KEYSTORE_TLS, doc = "TLS KeyStore password for proxy")
  private String tlsKeyStorePassword = null;

  @FieldContext(
    category = CATEGORY_KEYSTORE_TLS,
    doc = "TLS TrustStore type configuration for proxy: JKS, PKCS12"
  )
  private String tlsTrustStoreType = "JKS";

  @FieldContext(category = CATEGORY_KEYSTORE_TLS, doc = "TLS TrustStore path for proxy")
  private String tlsTrustStore = null;

  @FieldContext(category = CATEGORY_KEYSTORE_TLS, doc = "TLS TrustStore password for proxy")
  private String tlsTrustStorePassword = null;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc = "The maximum number of sessions which can exist concurrently on a connection."
  )
  private int amqpSessionCountLimit = 256;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "The default period with which Broker and client will exchange"
            + " heartbeat messages (in seconds). Clients may negotiate a different heartbeat"
            + " frequency or disable it altogether."
  )
  private int amqpHeartbeatDelay = 0;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "Factor to determine the maximum length of that may elapse between heartbeats being"
            + " received from the peer before a connection is deemed to have been broken."
  )
  private int amqpHeartbeatTimeoutFactor = 2;

  @FieldContext(category = CATEGORY_AMQP, doc = "Network buffer size.")
  // TODO: Network buffer size must be bigger than Netty's receive buffer. Also configure Netty with
  // this.
  private int amqpNetworkBufferSize = 2 * 1024 * 1024;

  @FieldContext(category = CATEGORY_AMQP, doc = "Max message size.")
  private int amqpMaxMessageSize = 100 * 1024 * 1024;

  @FieldContext(category = CATEGORY_AMQP, doc = "Length of binary data sent to debug log.")
  private int amqpDebugBinaryDataLength = 80;

  @FieldContext(
    category = CATEGORY_AMQP,
    doc =
        "Timeout in ms after which the connection closes even if a ConnectionCloseOk frame is not received"
  )
  private int amqpConnectionCloseTimeout = 2000;

  public Optional<Integer> getServicePort() {
    return servicePort;
  }

  public Optional<Integer> getServicePortTls() {
    return servicePortTls;
  }

  private Properties properties = new Properties();
}
