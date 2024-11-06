package com.saicone.delivery4j.broker;

import com.rabbitmq.client.*;
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
public class RabbitMQBroker extends Broker<RabbitMQBroker> {

    private final Connection connection;
    private final String exchange;

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
            cChannel = connection.createChannel();

            // Second!
            // Create auto-delete queue
            this.queue = cChannel.queueDeclare("", false, true, true, null).getQueue();
            // With auto-delete pre-channel (exchange in RabbitMQ)
            cChannel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, false, true, null);
            // And subscribed channels (routing keys in RabbitMQ)
            for (String channel : getSubscribedChannels()) {
                cChannel.queueBind(this.queue, exchange, channel);
            }

            // Third, and most important
            // Register callback for message delivery
            cChannel.basicConsume(this.queue, true, (consumerTag, message) -> {
                final String channel = message.getEnvelope().getRoutingKey();
                if (getSubscribedChannels().contains(channel)) {
                    receive(channel, message.getBody());
                }
            }, __ -> {}); // Without canceled delivery

            if (reconnected) {
                log(3, "RabbitMQ connection is alive again");
                reconnected = false;
            }
            setEnabled(true);
        } catch (Throwable t) {
            log(1, t);
            return;
        }

        // Maintain the connection alive
        if (aliveTask == null) {
            aliveTask = getExecutor().execute(this::alive, 30, 30, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void onClose() {
        close(cChannel, connection);
        cChannel = null;
        if (aliveTask != null) {
            getExecutor().cancel(aliveTask);
            aliveTask = null;
        }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                cChannel.queueBind(queue, exchange, channel);
            } catch (IOException e) {
                log(1, e);
            }
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                cChannel.queueUnbind(queue, exchange, channel);
            } catch (IOException e) {
                log(1, e);
            }
        }
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) {
        if (cChannel == null) {
            return;
        }

        try {
            // Publish to exchange and routing key without any special properties
            cChannel.basicPublish(exchange, channel, new AMQP.BasicProperties.Builder().build(), data);
        } catch (Throwable t) {
            log(2, t);
        }
    }

    @Override
    protected @NotNull RabbitMQBroker get() {
        return this;
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
        if (connection == null || !connection.isOpen() || cChannel == null || !cChannel.isOpen()) {
            close(cChannel, connection);
            cChannel = null;
            reconnected = true;
            setEnabled(false);
            while (true) {
                log(2, "RabbitMQ connection dropped, automatic reconnection every 8 seconds...");
                onStart();
                if (!isEnabled() && !Thread.interrupted()) {
                    try {
                        Thread.sleep(8000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    break;
                }
            }
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
            log(2, t);
        }
    }
}