package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * NATS broker implementation to publish data and consume it with subject subscription.
 *
 * @author Rubenicos
 */
public class NatsBroker extends Broker {

    private final Connection connection;

    private Dispatcher dispatcher;

    /**
     * Create a nats broker with provided parameters.
     *
     * @param host         the server host, wrapped as nats url.
     * @param userName     the username used to connect to server.
     * @param password     the password used along with username, in plain text.
     * @param reconnection true to set up a reconnection time of 8 seconds if connection fails, false otherwise.
     * @return             a newly generated nats broker instance.
     * @throws IOException if a networking issue occurs.
     * @throws InterruptedException if the current thread is interrupted.
     */
    @NotNull
    public static NatsBroker of(@NotNull String host, @Nullable String userName, @Nullable String password, boolean reconnection) throws IOException, InterruptedException {
        return of(host, userName, password, reconnection ? Duration.ofSeconds(8) : null);
    }

    /**
     * Create a nats broker with provided parameters.
     *
     * @param host         the server host, wrapped as nats url.
     * @param userName     the username used to connect to server.
     * @param password     the password used along with username, in plain text.
     * @param reconnection the reconnection duration interval to be used to reconnect to server, if connection fails.
     * @return             a newly generated nats broker instance.
     * @throws IOException if a networking issue occurs.
     * @throws InterruptedException if the current thread is interrupted.
     */
    @NotNull
    public static NatsBroker of(@NotNull String host, @Nullable String userName, @Nullable String password, @Nullable Duration reconnection) throws IOException, InterruptedException {
        return of(builder -> {
            final String serverURL;
            if (host.contains(":")) {
                serverURL = "nats://" + host;
            } else {
                serverURL = "nats://" + host + ":" + Options.DEFAULT_PORT;
            }
            builder.server(serverURL);
            if (userName != null && password != null) {
                builder.userInfo(userName, password);
            }
            if (reconnection != null) {
                builder.reconnectWait(reconnection);
                builder.maxReconnects(Integer.MAX_VALUE);
            }
        });
    }

    /**
     * Create a nats broker by providing builder options.
     *
     * @param consumer the operation that accept the builder of nats connection.
     * @return         a newly generated nats broker instance.
     * @throws IOException if a networking issue occurs.
     * @throws InterruptedException if the current thread is interrupted.
     */
    @NotNull
    public static NatsBroker of(@NotNull Consumer<Options.Builder> consumer) throws IOException, InterruptedException {
        final Options.Builder builder = new Options.Builder();
        consumer.accept(builder);
        return new NatsBroker(Nats.connect(builder.build()));
    }

    /**
     * Constructs a nats broker with provided connection.
     *
     * @param connection the nats server connection.
     */
    public NatsBroker(@NotNull Connection connection) {
        this.connection = connection;
    }

    @Override
    protected void onStart() {
        this.dispatcher = this.connection.createDispatcher(msg -> {
            final String channel = msg.getSubject();
            try {
                receive(channel, msg.getData());
            } catch (Throwable t) {
                getLogger().log(2, "Cannot process received message from channel '" + channel + "'", t);
            }
        });
        for (String channel : getSubscribedChannels()) {
            this.dispatcher.subscribe(channel);
        }
    }

    @Override
    protected void onClose() {
        try {
            this.connection.close();
        } catch (Throwable ignored) { }
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        for (@NotNull String channel : channels) {
            this.dispatcher.subscribe(channel);
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        for (@NotNull String channel : channels) {
            this.dispatcher.unsubscribe(channel);
        }
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        try {
            this.connection.publish(channel, data);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    /**
     * Get the current connection.
     *
     * @return a nats serve connection.
     */
    @NotNull
    public Connection getConnection() {
        return connection;
    }

    /**
     * Get the current dispatcher.
     *
     * @return the dispatcher used to listen connections.
     */
    @NotNull
    public Dispatcher getDispatcher() {
        return dispatcher;
    }
}
