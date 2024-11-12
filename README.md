<h1 align="center">Delivery4j</h1>

<h4 align="center">Java library wrapper for multiple data delivery concepts.</h4>

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
    <a href="https://jitpack.io/#com.saicone/delivery4j">
        <img src="https://jitpack.io/v/com.saicone/delivery4j.svg?style=flat-square"/>
    </a>
    <a href="https://javadoc.saicone.com/delivery4j/overview-summary.html">
        <img src="https://img.shields.io/badge/JavaDoc-Online-green?style=flat-square"/>
    </a>
    <a href="https://docs.saicone.com/delivery4j/">
        <img src="https://img.shields.io/badge/Saicone-delivery4j%20Wiki-3b3bb0?logo=github&logoColor=white&style=flat-square"/>
    </a>
</p>

There are multiple ways to transfer data between Java applications, this library offers an easy way to connect with them using common methods.

Currently supporting the brokers:

* [Kafka](https://github.com/apache/kafka) using empty-key records to producers.
* [NATS](https://github.com/nats-io/nats.java) using subject subscription.
* [PostgreSQL](https://github.com/pgjdbc/pgjdbc) using `LISTEN` and `NOTIFY` statement.
* [RabbitMQ](https://github.com/rabbitmq/rabbitmq-java-client) using queue and consumer via exchange.
* [Redis](https://github.com/redis/jedis) using publish and subscribe.
* SQL polling (not a real broker, but can be used as one).

PostgreSQL and SQL are also compatible with [Hikari](https://github.com/brettwooldridge/HikariCP).