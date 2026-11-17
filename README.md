<h1 align="center">Delivery4j</h1>

<h4 align="center">Java library facade for multiple data delivery concepts.</h4>

<p align="center">
    <a href="https://saic.one/discord">
        <img src="https://img.shields.io/discord/974288218839191612.svg?style=flat-square&label=discord&logo=discord&logoColor=white&color=7289da"/>
    </a>
    <a href="https://www.codefactor.io/repository/github/saicone/delivery4j">
        <img src="https://www.codefactor.io/repository/github/saicone/delivery4j/badge?style=flat-square"/>
    </a>
    <a href="https://github.com/saicone/delivery4j">
        <img src="https://img.shields.io/github/languages/code-size/saicone/delivery4j?logo=github&logoColor=white&style=flat-square"/>
    </a>
    <a href="https://central.sonatype.com/artifact/com.saicone/delivery4j">
        <img src="https://img.shields.io/maven-central/v/com.saicone/delivery4j"/>
    </a>
    <a href="https://javadoc.saicone.com/delivery4j/overview-summary.html">
        <img src="https://img.shields.io/badge/JavaDoc-Online-green?style=flat-square"/>
    </a>
    <a href="https://docs.saicone.com/delivery4j/">
        <img src="https://img.shields.io/badge/Saicone-delivery4j%20Wiki-3b3bb0?logo=github&logoColor=white&style=flat-square"/>
    </a>
</p>

There are multiple ways to transfer data between Java applications, this library offers an easy way to connect with them using common methods.

```java
// Create a broker instance to transfer data
// There is a plenty number of broker implementations, in this case we are using JedisBroker to connect to Redis database
final Broker broker = JedisBroker.simple("redis://:password@localhost:6379/0");
// (optional) Provide your own implementation if TaskExecutor
broker.setExecutor(TaskExecutor.from(ExecutorService, ScheduledExecutorService));
// (optional) Use your application logger with a max logging level
broker.setLogger(LogFilter.valueOf(logger, LogFilter.INFO));
// (optional) In case the broker is instance of PlainTextBroker, you can use a different byte codec implementation
broker.setCodec(ByteCodec.Z85);

// Create a messenger instance to send and receive messages
final Messenger messenger = Messenger.simple(broker);

// Subscribe to channel, for example, named "myapp:channel"
messenger.subscribe("myapp:channel")
         // (optional) Append a listener to receive messages from the channel
         .listener((channel, lines) -> {
             // do something
         })
         // (optional) Provide a identifier to avoid receiving self-produced messages
         .identifier(DataIdentifier.bit32())
         // (optional) Provide an encryptor to make end-to-end encryption
         .encryptor(Encryptor.of(key));

// Start the messenger instance to make it work
messenger.start();

// Send a multi-line message to the channel
// So your application on the other side can receive it
messenger.send("myapp:channel", "first line", "second line", "third line");

// Or send it asynchronously
messenger.sendAsync("myapp:channel", "first line", "second line", "third line");

// On shutdown, close the messenger instance to release resources
messenger.close();
// You can also clear the instance to remove all subscriptions and listeners
messenger.clear();
```

Currently supporting the brokers:

* [ActiveMQ](https://github.com/apache/activemq) using topic producers and consumers.
* [Kafka](https://github.com/apache/kafka) using empty-key records to producers.
* [NATS](https://github.com/nats-io/nats.java) using subject subscription.
* [PostgreSQL](https://github.com/pgjdbc/pgjdbc) using `LISTEN` and `NOTIFY` statement.
* [RabbitMQ](https://github.com/rabbitmq/rabbitmq-java-client) using queue and consumer via exchange.
* [Redis](https://github.com/redis/jedis) using publish and subscribe (also compatible with [KeyDB](https://github.com/Snapchat/KeyDB)).
* SQL polling (not a real broker, but can be used as one).
* [Valkey](https://github.com/valkey-io/valkey-java) using publish and subscribe (same as Redis, but with an older API).

PostgreSQL and SQL are also compatible with [Hikari](https://github.com/brettwooldridge/HikariCP).

## Dependency

Delivery4j contains the following artifacts:

* `delivery4j-api` - The main project.
* `delivery4j-broker-activemq` - ActiveMQ broker.
* `delivery4j-broker-kafka` - Kafka broker.
* `delivery4j-broker-nats` - NATS broker.
* `delivery4j-broker-postgresql` - PostgreSQL broker using plain Java connections.
* `delivery4j-broker-postgresql-hikari` - PostgreSQL broker using Hikari library.
* `delivery4j-broker-rabbitmq` - RabbitMQ broker.
* `delivery4j-broker-redis-jedis` - Redis broker using [jedis](https://github.com/redis/jedis) library.
* `delivery4j-broker-redis-lettuce` - Redis broker using [lettuce](https://github.com/redis/lettuce) library.
* `delivery4j-broker-sql` - SQL broker using plain Java connections.
* `delivery4j-broker-sql-hikari` - SQL broker using Hikari library.
* `delivery4j-broker-valkey` - Valkey broker.

> [!NOTE]
> If you still on Java 8 and want to use a `-hikari` artifact, filter the hikari sub-dependency and manually add hikari 4.x to your project.

build.gradle
```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'com.saicone:delivery4j-api:1.1.6'
}
```

build.gradle.kts
```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("com.saicone:delivery4j-api:1.1.6")
}
```

pom.xml
```xml
<dependencies>
    <dependency>
        <groupId>com.saicone</groupId>
        <artifactId>delivery4j-api</artifactId>
        <version>1.1.6</version>
        <scope>compile</scope>
    </dependency>
</dependencies>
```

## Usage

How to use Delivery4j library.

### Broker

Using brokers is pretty simple, you just need to create a broker instance (depending on the implementation) and set a consumer.

```java
Broker broker = // Create instance from any implementation

// Subscribe to channels
broker.subscribe("hello:world", "myChannel1");

broker.addListener((channel, data) -> {
    // do something
});

// Start connection
broker.start();

// Send data
byte[] data = ...;

broker.send("myChannel1", data);
```

Some brokers require to convert bytes to String and viceversa, Base64 is used by default.

```java
PlainTextBroker broker = // Create instance from any implementation

broker.setCodec(new ByteCodec<>() {
    @Override
    public @NotNull String encode(byte[] src) {
        // convert bytes to String
    }

    @Override
    public byte[] decode(@NotNull String src) {
        // convert String to bytes
    }
});
```

Some brokers have blocking operations or repetitive tasks, it's suggested to implement your own executor.

```java
Broker broker = // Create instance from any implementation

broker.setExecutor(new TaskExecutor<MyTaskObject>() {
    @Override
    public @NotNull MyTaskObject run(@NotNull Runnable command) {
        // run task and return itself
    }

    @Override
    public @NotNull MyTaskObject runLater(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        // run delayed task and return itself
    }

    @Override
    public @NotNull MyTaskObject runTimer(@NotNull Runnable command, long delay, long period, @NotNull TimeUnit unit) {
        // run repetitive task and return itself
    }

    @Override
    public void cancel(@NotNull MyTaskObject unused) {
        // cancel task
    }
});
```

And also a logging instance to log information about connection and exceptions, by default it use the best available implementation.

It uses a number terminology for logging levels:

1. Error
2. Warning
3. Information
4. Debug

```java
Broker broker = ...;

// --- Using existing logger instance
Logger logger = ...;
broker.setLogger(LogFilter.valueOf(logger, 3)); // using 3 as max logging level
broker.setLogger(LogFilter.valueOf(logger, () -> 3)); // you can also supply the max level dynamically

// --- Creating a logger instance from name
broker.setLogger(LogFilter.valueOf("LogName", 3));
broker.setLogger(LogFilter.valueOf("LogName", () -> 3));

// --- Creating a logger instance from class
broker.setLogger(LogFilter.valueOf(MyObject.class, 3));
broker.setLogger(LogFilter.valueOf(MyObject.class, () -> 3));
```

### Messenger

Probably the reason why you are here, it's a simple usage of brokers to send and receive multi-line String messages.

```java
Broker broker = ...;
Messenger messenger = Messenger.simple(broker);

// Start connection
messenger.start();

// Send multi-line message to channel
messenger.send("myChannel1", "Hello", "World");

// Subscribe to channel
messenger.subscribe("myChannel1").listener((channel, lines) -> {
    // do something
});
```

The subscribed message channels can have an identifier instance to avoid receive outbound messages.

```java
Broker broker = ...;
Messenger messenger = Messenger.simple(broker);

// Subscribe to channel
MessageChannel channel = messenger.subscribe("myChannel1").listener((channel, lines) -> {
    // do something
});

// Provide a 32-bit identifier
channel.identifier(DataIdentifier.bit32());

// Provide a 64-bit identifier (only needed if your application can send more than 32767 messages per second)
channel.identifier(DataIdentifier.bit64());
```

And also can have an end-to-end encryption.

```java
Broker broker = ...;
Messenger messenger = Messenger.simple(broker);

// Subscribe to channel
MessageChannel channel = messenger.subscribe("myChannel1").listener((channel, lines) -> {
    // do something
});

// Your key
SecretKey key = ...;

channel.encryptor(Encryptor.of(key));
```