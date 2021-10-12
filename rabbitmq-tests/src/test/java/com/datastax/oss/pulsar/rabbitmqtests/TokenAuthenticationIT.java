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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.rabbitmqgw.ConfigurationUtils;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayConfiguration;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayService;
import com.datastax.oss.pulsar.rabbitmqtests.utils.PulsarCluster;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.impl.DefaultCredentialsProvider;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TokenAuthenticationIT {
  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private static ConnectionFactory factory;

  private static final SecretKey SECRET_KEY =
      AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
  private final String CLIENT_ROLE = "client";
  private final String CLIENT_TOKEN =
      Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
    GatewayConfiguration config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    int port = PortManager.nextFreePort();
    config.setAmqpListeners(Collections.singleton("amqp://127.0.0.1:" + port));
    config.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());

    config.setAuthenticationEnabled(true);
    config
        .getProperties()
        .setProperty(
            "tokenSecretKey",
            "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));

    gatewayService =
        new GatewayService(
            config, new AuthenticationService(ConfigurationUtils.convertFrom(config)));
    gatewayService.start();

    factory = new ConnectionFactory();
    factory.setPort(port);
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testTokenAuthenticationSuccess() throws Exception {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("", CLIENT_TOKEN));
    Connection conn = factory.newConnection();
    assertTrue(conn.isOpen());
  }

  @Test
  void testTokenAuthenticationInvalidUser() {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("nobody", CLIENT_TOKEN));
    assertThrows(PossibleAuthenticationFailureException.class, factory::newConnection);
  }

  @Test
  void testTokenAuthenticationInvalidToken() {
    SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    String jwt = Jwts.builder().setSubject(CLIENT_ROLE).signWith(secretKey).compact();
    factory.setCredentialsProvider(new DefaultCredentialsProvider("", jwt));
    assertThrows(PossibleAuthenticationFailureException.class, factory::newConnection);
  }
}
