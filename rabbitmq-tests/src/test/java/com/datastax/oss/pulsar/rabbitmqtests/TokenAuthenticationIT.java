package com.datastax.oss.pulsar.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TokenAuthenticationIT {
  @TempDir
  public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private static ConnectionFactory factory;

  private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
  private final String CLIENT_ROLE = "client";
  private final String CLIENT_TOKEN = Jwts.builder().setSubject(CLIENT_ROLE).signWith(SECRET_KEY).compact();

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
    GatewayConfiguration config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setBrokerWebServiceURL(cluster.getAddress());
    config.setServicePort(Optional.of(PortManager.nextFreePort()));
    config.setZookeeperServers(cluster.getService().getConfig().getZookeeperServers());

    config.setAuthenticationEnabled(true);
    config.getProperties().setProperty("tokenSecretKey", "data:;base64," + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
    Set<String> providers = new HashSet<>();
    providers.add(AuthenticationProviderToken.class.getName());
    config.setAuthenticationProviders(providers);

    gatewayService = new GatewayService(config, new AuthenticationService(PulsarConfigurationLoader.convertFrom(config)));
    gatewayService.start();

    factory = new ConnectionFactory();
    factory.setVirtualHost("/");
    factory.setHost("localhost");
    factory.setPort(config.getServicePort().get());
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
    factory.setCredentialsProvider(new DefaultCredentialsProvider("token", CLIENT_TOKEN));
    Connection conn = factory.newConnection();
    assertTrue(conn.isOpen());
  }

  @Test
  void testTokenAuthenticationInvalidUser() throws Exception {
    factory.setCredentialsProvider(new DefaultCredentialsProvider("nobody", CLIENT_TOKEN));
    assertThrows(PossibleAuthenticationFailureException.class, () -> factory.newConnection());
  }

  @Test
  void testTokenAuthenticationInvalidToken() throws Exception {
    SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    String jwt = Jwts.builder().setSubject(CLIENT_ROLE).signWith(secretKey).compact();
    factory.setCredentialsProvider(new DefaultCredentialsProvider("token", jwt));
    assertThrows(PossibleAuthenticationFailureException.class, () -> factory.newConnection());
  }

}
