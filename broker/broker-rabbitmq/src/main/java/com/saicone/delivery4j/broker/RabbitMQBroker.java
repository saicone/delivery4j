package com.saicone.delivery4j.broker;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.saicone.delivery4j.Broker;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ integration for data delivery.
 *
 * @author Rubenicos
 */
public class RabbitMQBroker extends Broker {

    private final Connection connection;
    private final String exchange;

    private long checkTime = 8;
    private TimeUnit checkUnit = TimeUnit.SECONDS;

    private Channel cChannel = null;
    private String queue = null;

    private Object aliveTask = null;
    private boolean reconnected = false;

    /**
     * Create a RabbitMQDelivery client with provided parameters.
     *
     * @param url      the URL to connect with.
     * @param exchange the pre-channel to use.
     * @return         new RabbitMQDelivery instance.
     */
    @NotNull
    public static RabbitMQBroker of(@NotNull String url, @NotNull String exchange) {
        try {
            return of(new URI(url), exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a RabbitMQDelivery client with provided parameters.
     *
     * @param uri      the URL object to connect with.
     * @param exchange the pre-channel to use.
     * @return         new RabbitMQDelivery instance.
     */
    @NotNull
    public static RabbitMQBroker of(@NotNull URI uri, @NotNull String exchange) {
        final ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
            return new RabbitMQBroker(factory.newConnection(), exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a RabbitMQDelivery client with provided parameters.
     *
     * @param host        the host to connect.
     * @param port        the port host.
     * @param username    the username to validate authentication.
     * @param password    the password to validate authentication.
     * @param virtualHost the virtual host.
     * @param exchange    the pre-channel to use.
     * @return            new RabbitMQDelivery instance.
     */
    @NotNull
    public static RabbitMQBroker of(@NotNull String host, int port, @NotNull String username, @NotNull String password, @NotNull String virtualHost, @NotNull String exchange) {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        try {
            return new RabbitMQBroker(factory.newConnection(), exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs a RabbitMQDelivery with provided parameters.
     *
     * @param connection the connection to interact with.
     * @param exchange   the pre-channel to use.
     */
    public RabbitMQBroker(@NotNull Connection connection, @NotNull String exchange) {
        this.connection = connection;
        this.exchange = exchange;
    }

    @Override
    protected void onStart() {
        // Random stuff, let's go!
        try {
            // First, create a channel
            this.cChannel = this.connection.createChannel();

            // Second!
            // Create auto-delete queue
            this.queue = this.cChannel.queueDeclare("", false, true, true, null).getQueue();
            // With auto-delete pre-channel (exchange in RabbitMQ)
            this.cChannel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, false, true, null);
            // And subscribed channels (routing keys in RabbitMQ)
            for (String channel : getSubscribedChannels()) {
                this.cChannel.queueBind(this.queue, this.exchange, channel);
            }

            // Third, and most important
            // Register callback for message delivery
            this.cChannel.basicConsume(this.queue, true, (consumerTag, message) -> {
                final String channel = message.getEnvelope().getRoutingKey();
                if (getSubscribedChannels().contains(channel)) {
                    receive(channel, message.getBody());
                }
            }, __ -> {}); // Without canceled delivery

            if (this.reconnected) {
                getLogger().log(3, "RabbitMQ connection is alive again");
                this.reconnected = false;
            }
            setEnabled(true);
        } catch (Throwable t) {
            getLogger().log(1, "Cannot start RabbitMQ connection", t);
            return;
        }

        // Maintain the connection alive
        if (this.aliveTask == null) {
            this.aliveTask = getExecutor().execute(this::alive, this.checkTime, this.checkTime, this.checkUnit);
        }
    }

    @Override
    protected void onClose() {
        close(this.cChannel, this.connection);
        this.cChannel = null;
        if (this.aliveTask != null) {
            getExecutor().cancel(this.aliveTask);
            this.aliveTask = null;
        }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                this.cChannel.queueBind(this.queue, this.exchange, channel);
            } catch (IOException e) {
                getLogger().log(1, "Cannot subscribe to channel '" + channel + "'", e);
            }
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                this.cChannel.queueUnbind(this.queue, this.exchange, channel);
            } catch (IOException e) {
                getLogger().log(1, "Cannot unsubscribe from channel '" + channel + "'", e);
            }
        }
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        if (this.cChannel == null) {
            return;
        }

        try {
            // Publish to exchange and routing key without any special properties
            this.cChannel.basicPublish(this.exchange, channel, new AMQP.BasicProperties.Builder().build(), data);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public void setReconnectionInterval(int time, @NotNull TimeUnit unit) {
        this.checkTime = time;
        this.checkUnit = unit;
    }

    /**
     * The current connection.
     *
     * @return a RabbitMQ connection object.
     */
    @NotNull
    public Connection getConnection() {
        return connection;
    }

    @SuppressWarnings("all")
    private void alive() {
        if (!isEnabled()) {
            return;
        }
        if (!this.connection.isOpen() || this.cChannel == null || !this.cChannel.isOpen()) {
            close(this.cChannel, this.connection);
            this.cChannel = null;
            this.reconnected = true;
            setEnabled(false);
            getLogger().log(2, () -> "RabbitMQ connection dropped, automatic reconnection every " + this.checkTime + " " + this.checkUnit.name().toLowerCase() + "...");
            onStart();
        }
    }

    private void close(AutoCloseable... closeables) {
        try {
            for (AutoCloseable closeable : closeables) {
                if (closeable != null) {
                    closeable.close();
                }
            }
        } catch (Throwable t) {
            getLogger().log(2, "Cannot close RabbitMQ connection", t);
        }
    }
}