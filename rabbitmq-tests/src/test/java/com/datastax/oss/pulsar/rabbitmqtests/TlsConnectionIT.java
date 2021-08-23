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
package com.datastax.oss.pulsar.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.pulsar.rabbitmqgw.GatewayConfiguration;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayService;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayServiceStarter;
import com.datastax.oss.pulsar.rabbitmqtests.utils.PulsarCluster;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
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

  private static final String CLIENT_KEYSTORE_CN = "clientuser";
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
  public void beforeEach() throws PulsarServerException {
    int port = PortManager.nextFreePort();

    config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    config.setServicePortTls(Optional.of(port));
    config.setZookeeperServers(cluster.getService().getConfig().getZookeeperServers());

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

    gatewayService =
        new GatewayService(
            config, new AuthenticationService(PulsarConfigurationLoader.convertFrom(config)));

    factory = new ConnectionFactory();
    factory.setPort(port);
  }

  @AfterEach
  public void afterEach() throws IOException {
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testTlsConnectionSuccess() throws Exception {
    gatewayService.start();

    SSLContext sslCtx =
        SecurityUtility.createSslContext(
            false, TLS_TRUST_CERT_FILE_PATH, TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH);
    factory.useSslProtocol(sslCtx);

    factory.newConnection();
  }

  @Test
  void testTlsConnectionFailure() throws Exception {
    gatewayService.start();

    assertThrows(IOException.class, factory::newConnection);
  }

  @Test
  void testKeyStoreTlsConnectionSuccess() throws Exception {
    config.setTlsEnabledWithKeyStore(true);
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
    gatewayService.start();

    assertThrows(IOException.class, factory::newConnection);
  }

  @Test
  void testTlsAuthenticationSuccess() throws Exception {
    config.setAuthenticationEnabled(true);
    config.setAuthenticationMechanisms(Collections.singleton("EXTERNAL"));
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(GatewayServiceStarter.convertFrom(config)));
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
    config.setAuthenticationMechanisms(Collections.singleton("EXTERNAL"));
    gatewayService =
        new GatewayService(
            config, new AuthenticationService(GatewayServiceStarter.convertFrom(config)));
    gatewayService.start();

    SSLContext sslCtx =
        SecurityUtility.createSslContext(false, TLS_TRUST_CERT_FILE_PATH, null, null);
    factory.useSslProtocol(sslCtx);
    factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);

    assertThrows(PossibleAuthenticationFailureException.class, factory::newConnection);
  }

  private void setTlsConfig(boolean withKeyStore) {
    if (withKeyStore) {
      config.setTlsEnabledWithKeyStore(true);
    }
  }
}
