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
package com.datastax.oss.starlight.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.starlight.rabbitmq.ConfigurationUtils;
import com.datastax.oss.starlight.rabbitmq.GatewayConfiguration;
import com.datastax.oss.starlight.rabbitmq.GatewayService;
import com.datastax.oss.starlight.rabbitmqtests.utils.PulsarCluster;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import javax.net.ssl.SSLContext;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TlsConnectionIT {

  private static final String TLS_TRUST_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/cacert.pem";
  private static final String TLS_GATEWAY_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/server-cert.pem";
  private static final String TLS_GATEWAY_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/server-key.pem";
  private static final String TLS_CLIENT_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/client-cert.pem";
  private static final String TLS_CLIENT_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/client-key.pem";

  private static final String BROKER_KEYSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
  private static final String BROKER_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
  private static final String BROKER_KEYSTORE_PW = "111111";
  private static final String BROKER_TRUSTSTORE_PW = "111111";

  private static final String CLIENT_KEYSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/client.keystore.jks";
  private static final String CLIENT_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/client.truststore.jks";
  private static final String CLIENT_KEYSTORE_PW = "111111";
  private static final String CLIENT_TRUSTSTORE_PW = "111111";

  private static final String KEYSTORE_TYPE = "JKS";

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;

  private GatewayService gatewayService;
  private ConnectionFactory factory;
  private GatewayConfiguration config;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @BeforeEach
  public void beforeEach() {
    CollectorRegistry.defaultRegistry.clear();
    int port = PortManager.nextFreePort();

    config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    config.setAmqpListeners(Collections.singleton("amqps://127.0.0.1:" + port));
    config.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());

    config.setTlsKeyStoreType(KEYSTORE_TYPE);
    config.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
    config.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
    config.setTlsTrustStoreType(KEYSTORE_TYPE);
    config.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
    config.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

    config.setTlsCertificateFilePath(TLS_GATEWAY_CERT_FILE_PATH);
    config.setTlsKeyFilePath(TLS_GATEWAY_KEY_FILE_PATH);
    config.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

    config.setTlsRequireTrustedClientCertOnConnect(true);

    factory = new ConnectionFactory();
    factory.setPort(port);
    factory.enableHostnameVerification();
  }

  @AfterEach
  public void afterEach() throws IOException {
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testTlsConnectionSuccess() throws Exception {
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    SSLContext sslCtx =
        SecurityUtility.createSslContext(
            false, TLS_TRUST_CERT_FILE_PATH, TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH);
    factory.useSslProtocol(sslCtx);

    factory.newConnection();
  }

  @Test
  void testTlsConnectionFailure() throws Exception {
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    assertThrows(IOException.class, factory::newConnection);
  }

  @Test
  void testKeyStoreTlsConnectionSuccess() throws Exception {
    config.setTlsEnabledWithKeyStore(true);
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    SSLContext sslCtx =
        KeyStoreSSLContext.createClientSslContext(
            null,
            KEYSTORE_TYPE,
            CLIENT_KEYSTORE_FILE_PATH,
            CLIENT_KEYSTORE_PW,
            false,
            KEYSTORE_TYPE,
            BROKER_TRUSTSTORE_FILE_PATH,
            BROKER_TRUSTSTORE_PW,
            null,
            null);
    factory.useSslProtocol(sslCtx);

    factory.newConnection();
  }

  @Test
  void testKeyStoreTlsConnectionFailure() throws Exception {
    config.setTlsEnabledWithKeyStore(true);
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    assertThrows(IOException.class, factory::newConnection);
  }

  @Test
  void testTlsAuthenticationSuccess() throws Exception {
    config.setAuthenticationEnabled(true);
    config.setAmqpAuthenticationMechanisms(Collections.singleton("EXTERNAL"));
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    SSLContext sslCtx =
        SecurityUtility.createSslContext(
            false, TLS_TRUST_CERT_FILE_PATH, TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH);
    factory.useSslProtocol(sslCtx);
    factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);

    factory.newConnection();
  }

  @Test
  void testTlsAuthenticationFailure() throws Exception {
    config.setTlsRequireTrustedClientCertOnConnect(false);
    config.setAuthenticationEnabled(true);
    config.setAmqpAuthenticationMechanisms(Collections.singleton("EXTERNAL"));
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    SSLContext sslCtx =
        SecurityUtility.createSslContext(false, TLS_TRUST_CERT_FILE_PATH, null, null);
    factory.useSslProtocol(sslCtx);
    factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);

    assertThrows(PossibleAuthenticationFailureException.class, factory::newConnection);
  }
}
