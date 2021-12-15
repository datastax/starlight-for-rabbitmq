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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.starlight.rabbitmq.ConfigurationUtils;
import com.datastax.oss.starlight.rabbitmq.GatewayConfiguration;
import com.datastax.oss.starlight.rabbitmq.GatewayService;
import com.datastax.oss.starlight.rabbitmqtests.utils.PulsarCluster;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.DefaultCredentialsProvider;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AuthorizationIT {

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private static ConnectionFactory factory;

  private static final SecretKey SECRET_KEY =
      AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
  private static final String CLIENT_ROLE = "client";
  private final String CLIENT_TOKEN =
      Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();
  private static final String ADMIN_ROLE = "admin";
  private final String ADMIN_TOKEN =
      Jwts.builder().setSubject(ADMIN_ROLE).signWith(SECRET_KEY).compact();

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();

    cluster
        .getService()
        .getAdminClient()
        .namespaces()
        .grantPermissionOnNamespace(
            "public/default",
            CLIENT_ROLE,
            new HashSet<>(Arrays.asList(AuthAction.consume, AuthAction.produce)));

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

    config.setAuthorizationEnabled(true);

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
  void testAuthorizationSuccess() throws Exception {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("", ADMIN_TOKEN));
    factory.setVirtualHost("/");
    Connection conn = factory.newConnection();
    assertTrue(conn.isOpen());
  }

  @Test
  void testAuthorizationUnknownVhost() {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("", ADMIN_TOKEN));
    factory.setVirtualHost("unknown");
    assertThrows(IOException.class, () -> factory.newConnection());
  }

  @Test
  void testTokenAuthenticationUnauthorizedUser() {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("", CLIENT_TOKEN));
    factory.setVirtualHost("/");
    assertThrows(IOException.class, () -> factory.newConnection());
  }
}
