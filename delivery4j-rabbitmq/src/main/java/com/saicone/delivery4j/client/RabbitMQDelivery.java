package com.saicone.delivery4j.client;

import com.rabbitmq.client.*;
import com.saicone.delivery4j.DeliveryClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ integration for data delivery.
 *
 * @author Rubenicos
 */
public class RabbitMQDelivery extends DeliveryClient {

    private final Connection connection;
    private final String exchange;

    private Channel cChannel = null;
    private String queue = null;

    private Runnable aliveTask = null;
    private boolean reconnected = false;

    /**
     * Create a RabbitMQDelivery client with provided parameters.
     *
     * @param url      the URL to connect with.
     * @param exchange the pre-channel to use.
     * @return         new RabbitMQDelivery instance.
     */
    @NotNull
    public static RabbitMQDelivery of(@NotNull String url, @NotNull String exchange) {
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
    public static RabbitMQDelivery of(@NotNull URI uri, @NotNull String exchange) {
        final ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
            return new RabbitMQDelivery(factory.newConnection(), exchange);
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
    public static RabbitMQDelivery of(@NotNull String host, int port, @NotNull String username, @NotNull String password, @NotNull String virtualHost, @NotNull String exchange) {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        try {
            return new RabbitMQDelivery(factory.newConnection(), exchange);
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
    public RabbitMQDelivery(@NotNull Connection connection, @NotNull String exchange) {
        this.connection = connection;
        this.exchange = exchange;
    }

    @Override
    public void onStart() {
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
            for (String channel : subscribedChannels) {
                cChannel.queueBind(this.queue, exchange, channel);
            }

            // Third, and most important
            // Register callback for message delivery
            cChannel.basicConsume(this.queue, true, (consumerTag, message) -> {
                final String channel = message.getEnvelope().getRoutingKey();
                if (subscribedChannels.contains(channel)) {
                    receive(channel, message.getBody());
                }
            }, __ -> {}); // Without canceled delivery

            if (reconnected) {
                log(3, "RabbitMQ connection is alive again");
                reconnected = false;
            }
            enabled = true;
        } catch (Throwable t) {
            log(1, t);
            return;
        }

        // Maintain the connection alive
        if (aliveTask == null) {
            aliveTask = asyncRepeating(this::alive, 30, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onClose() {
        close(cChannel, connection);
        cChannel = null;
        if (aliveTask != null) {
            aliveTask.run();
            aliveTask = null;
        }
    }

    @Override
    public void onSubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                cChannel.queueBind(queue, exchange, channel);
            } catch (IOException e) {
                log(1, e);
            }
        }
    }

    @Override
    public void onUnsubscribe(@NotNull String... channels) {
        for (String channel : channels) {
            try {
                cChannel.queueUnbind(queue, exchange, channel);
            } catch (IOException e) {
                log(1, e);
            }
        }
    }

    @Override
    public void onSend(@NotNull String channel, byte[] data) {
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
        if (!enabled) {
            return;
        }
        if (connection == null || !connection.isOpen() || cChannel == null || !cChannel.isOpen()) {
            close(cChannel, connection);
            cChannel = null;
            reconnected = true;
            enabled = false;
            while (true) {
                log(2, "RabbitMQ connection dropped, automatic reconnection every 8 seconds...");
                onStart();
                if (!enabled && !Thread.interrupted()) {
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