# Starlight for RabbitMQ

Starlight for RabbitMQ acts as a proxy between your RabbitMQ application and Apache Pulsar.
It implements the AMQP 0.9.1 protocol used by RabbitMQ clients and translates AMQP frames and concepts to Pulsar ones.
The proxy can be run as a standalone jar, a Pulsar
[Pluggable Protocol Handler](https://github.com/apache/pulsar/wiki/PIP-41%3A-Pluggable-Protocol-Handler) 
 or a Pulsar [Proxy extension](https://github.com/apache/pulsar/wiki/PIP-99%3A-Pulsar-Proxy-Extensions).

## Limitations

This is currently not implemented but on the roadmap:
* Topic and headers exchanges
* Exclusive consumers
* Non durable exchanges and queues
* Transient messages (all messages are persisted)
* Authorization support

RabbitMQ and Pulsar work in a pretty different way.
Starlight for RabbitMQ was designed to make the most benefit from Pulsar's scalability.
This results in some differences of behavior:
* Canceling an AMQP consumer will requeue the messages that were received through it since it also closes
the associated Pulsar consumers.

## Get started

### Download and build Starlight for RabbitMQ

To build from code, complete the following steps:
1. Clone the project from GitHub.

```bash
git clone https://github.com/datastax/starlight-for-rabbitmq.git
cd starlight-for-rabbitmq
```

2. Build the project.
```bash
mvn clean install -DskipTests
```

You can find the executable jar file in the following directory.
```bash
./starlight-rabbitmq/target/starlight-rabbitmq-${version}-jar-with-dependencies.jar
```
You can find the nar file in the following directory.
```bash
./starlight-rabbitmq/target/starlight-rabbitmq-${version}.nar
```

### Running Starlight for RabbitMQ as a standalone executable jar

1. Set the URLs of the Pulsar brokers and the ZooKeeper configuration store in a configuration file. Eg:
   ```properties
   brokerServiceURL=pulsar://localhost:6650
   brokerWebServiceURL=http://localhost:8080
   configurationStoreServers=localhost:2181
   ```
2. Run as a Java application and provide the configuration file path in the `-c/--config` option:
   ```bash
   java -jar ./starlight-rabbitmq/target/starlight-rabbitmq-${version}-jar-with-dependencies.jar -c conf/starlight-rabbitmq.conf
   ```

### Running Starlight for RabbitMQ as a protocol handler

Starlight for RabbitMQ can be embedded directly into the Pulsar brokers by loading it as a protocol handler.

1. Set the configuration of the Starlight for RabbitMQ protocol handler in the broker configuration file (generally `broker.conf` or `standalone.conf`).
   Example where the NAR file was copied into the `./protocols` directory:
    ```properties
   messagingProtocols=rabbitmq
   protocolHandlerDirectory=./protocols
    ```

2. Set the AMQP service listeners. Note that the hostname value in listeners is the same as Pulsar broker's `advertisedAddress`. 
   The following is an example.
   ```properties
   amqpListeners=amqp://127.0.0.1:5672
   advertisedAddress=127.0.0.1
   ```

3. Start the Pulsar broker

### Running Starlight for RabbitMQ as a Pulsar Proxy extension

Starlight for RabbitMQ can be embedded into the Pulsar Proxy by loading it as a proxy extension.

1. Set the configuration of the Starlight for RabbitMQ protocol handler in the broker configuration file (generally `broker.conf` or `standalone.conf`).
   Example where the NAR file was copied into the `./protocols` directory:
    ```properties
   proxyExtensions=rabbitmq
   proxyExtensionsDirectory=./protocols
    ```

2. Set the AMQP service listeners. Note that the hostname value in listeners is the same as Pulsar broker's `advertisedAddress`.
   The following is an example.
   ```properties
   amqpListeners=amqp://127.0.0.1:5672
   advertisedAddress=127.0.0.1
   ```

3. Start the Pulsar Proxy

### Checking that it works

You can use a RabbitMQ/AMQP-0.9.1 client or a tool such as [RabbitMQ PerfTest](https://rabbitmq.github.io/rabbitmq-perf-test/stable/htmlsingle/)
to check that everything works correctly.

For instance the following Python script creates a queue, publishes a message that will be routed to this queue, reads the message from the queue and deletes the queue
```python
#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(port=5672))
channel = connection.channel()

try:
    channel.queue_declare("test-queue")
    print("created test-queue queue")
    channel.basic_publish(exchange="", routing_key="test-queue", body="test".encode('utf-8'))
    print("published message test")
    _, _, res = channel.basic_get(queue="test-queue", auto_ack=True)
    assert res is not None, "should have received a message"
    print("received message: " + res.decode())
    channel.queue_delete("test-queue")
    print("deleted test-queue queue")
finally:
    connection.close()
```

## Configuration

### Generic configuration
|Name|Description|Default|
|---|---|---|
|configurationStoreServers|Zookeeper configuration store connection string (as a comma-separated list)|
|amqpListeners|Used to specify multiple advertised listeners for the proxy. The value must format as `amqp[s]://<host>:<port>`, multiple listeners should be separated with commas.|amqp://127.0.0.1:5672
|amqpSessionCountLimit|The maximum number of sessions which can exist concurrently on an AMQP connection.|256
|amqpHeartbeatDelay|The default period with which Broker and client will exchange heartbeat messages (in seconds) when using AMQP. Clients may negotiate a different heartbeat frequency or disable it altogether.|0
|amqpHeartbeatTimeoutFactor|Factor to determine the maximum length of that may elapse between heartbeats being received from the peer before an AMQP0.9 connection is deemed to have been broken.|2
|amqpNetworkBufferSize|AMQP Network buffer size.|2097152 (2MB)
|amqpMaxMessageSize|AMQP Max message size.|104857600 (100MB)
|amqpDebugBinaryDataLength|AMQP Length of binary data sent to debug log.|80
|amqpConnectionCloseTimeout|Timeout in ms after which the AMQP connection closes even if a ConnectionCloseOk frame is not received|2000
|amqpBatchingEnabled|Whether batching messages is enabled in AMQP|true

### Authentication configuration
|Name|Description|Default|
|---|---|---|
|authenticationEnabled| Whether authentication is enabled for the proxy  |false|
|amqpAuthenticationMechanisms|Authentication mechanism name list for AMQP (a comma-separated list of mecanisms. Eg: PLAIN,EXTERNAL)|PLAIN
|tokenSecretKey| Configure the secret key to be used to validate auth tokens. The key can be specified like: `tokenSecretKey=data:;base64,xxxxxxxxx` or `tokenSecretKey=file:///my/secret.key`.  Note: key file must be DER-encoded.||
|tokenPublicKey| Configure the public key to be used to validate auth tokens. The key can be specified like: `tokenPublicKey=data:;base64,xxxxxxxxx` or `tokenPublicKey=file:///my/secret.key`. Note: key file must be DER-encoded.||
|tokenAuthClaim| Specify the token claim that will be used as the authentication "principal" or "role". The "subject" field will be used if this is left blank ||
|tokenAudienceClaim| The token audience "claim" name, e.g. "aud". It is used to get the audience from token. If it is not set, the audience is not verified. ||
|tokenAudience| The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token need contains this parameter.| |

### Broker client configuration
|Name|Description|Default|
|---|---|---|
|brokerServiceURL| The service URL pointing to the broker cluster. | |
|brokerWebServiceURL| The Web service URL pointing to the broker cluster | |
|brokerClientAuthenticationPlugin|  The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientAuthenticationParameters|  The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|amqpBrokerClientAuthenticationParameters|If set, the RabbitMQ service will use these parameters to authenticate on Pulsar's brokers. If not set, the brokerClientAuthenticationParameters setting will be used. This setting allows to have different credentials for the Pulsar proxy and for the RabbitMQ service|
|tlsEnabledWithBroker|  Whether TLS is enabled when communicating with Pulsar brokers. |false|
|brokerClientTrustCertsFilePath|  The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers ||
|brokerClientTlsEnabledWithKeyStore| Whether the proxy use KeyStore type to authenticate with Pulsar brokers ||
|brokerClientTlsTrustStoreType| TLS TrustStore type configuration for proxy: JKS, PKCS12  used by the proxy to authenticate with Pulsar brokers ||
|brokerClientTlsTrustStore| TLS TrustStore path for proxy,  used by the Pulsar proxy to authenticate with Pulsar brokers ||
|brokerClientTlsTrustStorePassword| TLS TrustStore password for proxy,  used by the Pulsar proxy to authenticate with Pulsar brokers ||

### TLS configuration
|Name|Description|Default|
|---|---|---|
|tlsCertRefreshCheckDurationSec| TLS certificate refresh duration in seconds. If the value is set 0, check TLS certificate every new connection. | 300 |
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||
|tlsTrustCertsFilePath| Path for the trusted TLS certificate pem file ||
|tlsAllowInsecureConnection| Accept untrusted TLS certificate from client. If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath` cert will be allowed to connect to the server, though the cert will not be used for client authentication ||
|tlsHostnameVerificationEnabled|  Whether the hostname is validated when the proxy creates a TLS connection with brokers  |false|
|tlsRequireTrustedClientCertOnConnect|  Whether client certificates are required for TLS. Connections are rejected if the client certificate isn’t trusted. |false|
|tlsProtocols|Specify the tls protocols the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLSv1.3```, ```TLSv1.2``` ||
|tlsCiphers|Specify the tls cipher the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256```||
|tlsRequireTrustedClientCertOnConnect| Whether client certificates are required for TLS. Connections are rejected if the client certificate isn't trusted ||
|tlsEnabledWithKeyStore| Enable TLS with KeyStore type configuration for proxy ||
|tlsProvider| TLS Provider ||
|tlsKeyStoreType| TLS KeyStore type configuration for proxy: JKS, PKCS12 ||
|tlsKeyStore| TLS KeyStore path for proxy ||
|tlsKeyStorePassword| TLS KeyStore password for proxy ||
|tlsTrustStoreType| TLS TrustStore type configuration for proxy: JKS, PKCS12 ||
|tlsTrustStore| TLS TrustStore path for proxy ||
|tlsTrustStorePassword| TLS TrustStore password for proxy ||

## Under the hood

AMQP 0.9.1 (the protocol used by RabbitMQ) employs the concepts of `Exchanges`, `Queues` and `Bindings` to provide basic routing capabilities inside the message broker.
These concepts are mapped to Pulsar topics and features.
One important architectural decision is that Starlight for RabbitMQ doesn’t interact directly with the managed ledger as in other RabbitMQ integrations for Pulsar.
Interacting with the ledger has the advantage of being performant, but the disadvantage is that the broker which interacts with the ledger must have ownership of the topic.
Since in AMQP 0.9.1 there is a many-to-many relationship between `Exchanges` and `Queues` for a given `Virtual host`, all `Exchanges` and `Queues` and related topics would have to be owned by the same broker.
There are techniques to do this using Topic bundles, but the result is that a full AMQP `Virtual host` `can be handled by only one broker at a time. This is an issue for scalability.
So instead, Starlight for RabbitMQ acts as a proxy and uses the Pulsar binary protocol to communicate with the brokers.
This way it can leverage Pulsar features such as load balancing of the topics on the brokers, batching of messages, partitioning of topics, and load balancing of the data on the consumers.

On the publishing side, an AMQP exchange is mapped to a topic. Depending on the type of exchange, the publishing routing key may also be included in the topic name.

On the consumer side, Pulsar shared subscriptions are used to represent the AMQP `Bindings` from an `Exchange` to a `Queue`.
When creating an AMQP Queue consumer, the proxy creates Pulsar consumers for all the `Bindings` of the `Queue`.

When you unbind the `Queue`, the Pulsar subscription isn’t deleted right away since the consumer may be lagging.
Messages are still received from the subscription and filtered if their position is past the end of the binding.
When all messages from the binding have been acknowledged, then the corresponding subscription can finally be removed by the `SubscriptionCleaner` task.

### Consistent metadata store

Starlight for RabbitMQ uses Apache Zookeeper to store the AMQP entities metadata consistently.
The existing ZooKeeper configuration store can be reused for this, and Starlight for RabbitMQ will employ the /starlight-rabbitmq prefix to write its entries into ZooKeeper.

### Security and authentication

Starlight for RabbitMQ supports connections using TLS/mTLS to ensure privacy and security of the communication.
It also supports the PLAIN and EXTERNAL mechanisms used by RabbitMQ.
Internally, it uses the same `AuthenticationService` as Apache Pulsar and maps the AMQP mechanisms to existing Pulsar authentication modes.
At the moment there is no support for authorization so an authenticated user has full access to all `Virtual hosts`.
Starlight for RabbitMQ can connect to brokers that have TLS and/or authentication, and/or authorization enabled.
To perform its operations, Starlight for RabbitMQ currently needs to use an “admin role”.
Future versions will relay the principal authenticated to the proxy and use a “proxy role” so operations on the broker will have permissions from the originating application.

#### PLAIN authentication mechanism

The PLAIN mechanism is mapped to the AuthenticationProviderToken mode of authentication. The username is ignored and the password is used as the JSON Web Token (JWT).

#### EXTERNAL authentication mechanism

The EXTERNAL mechanism is mapped to the AuthenticationProviderTls mode of authentication.
This is the equivalent of the [rabbitmq-auth-mechanism-ssl plugin](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_auth_mechanism_ssl) with `ssl_cert_login_from` parameter set to `common_name`.

### Clustering

Multiple Starlight for RabbitMQ proxies can be launched at the same time for scalability and high availability needs.
The proxies are stateless and can be started and stopped at will.
They share their configuration in Zookeeper so you can create/delete/bind/unbind exchanges and queues on any proxy, and the configuration will be synchronized on the other proxies.
Publishing messages can be done on any proxy.
On the receiving side, messages will be dispatched evenly to all connected AMQP consumers since the Pulsar subscriptions are shared ones.

### Multi-tenancy

Starlight for RabbitMQ offers support for multi-tenancy by mapping an AMQP `Virtual host` to a Pulsar `tenant` and `namespace`.
The mapping is done as follows:
* AMQP vhost `/` is mapped to Pulsar namespace `public/default`
* AMQP vhost `/<tenant>` or `<tenant>` is mapped to Pulsar namespace `public/<tenant>`
* AMQP vhost `/<tenant>/<namespace>` or `<tenant>/<namespace>` is mapped to Pulsar namespace `<tenant>/<namespace>`

This means that AMQP vhosts must only contain characters that are accepted in Pulsar tenant and namespace names (ie. `a-zA-Z0-9_-=:.`)

# Tests
## System tests

System tests is a test suite which runs a RabbitMQ client against a real Pulsar Cluster and tests the protocol handler is working correctly.

```
mvn -f rabbitmq-tests integration-test failsafe:verify -Dgroups=com.datastax.oss.starlight.rabbitmqtests.SystemTest \
   -Dtests.systemtests.enabled=true \
   -Dtests.systemtests.pulsar.host=your-pulsar-broker-or-proxy-hostname \
   -Dtests.systemtests.ampqlistener.port=5672
```


