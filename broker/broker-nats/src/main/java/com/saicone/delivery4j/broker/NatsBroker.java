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

public class NatsBroker extends Broker {

    private final Connection connection;

    private Dispatcher dispatcher;

    @NotNull
    public static NatsBroker of(@NotNull String host, @Nullable String userName, @Nullable String password, boolean reconnection) throws IOException, InterruptedException {
        return of(host, userName, password, reconnection ? Duration.ofSeconds(8) : null);
    }

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

    @NotNull
    public static NatsBroker of(@NotNull Consumer<Options.Builder> consumer) throws IOException, InterruptedException {
        final Options.Builder builder = new Options.Builder();
        consumer.accept(builder);
        return new NatsBroker(Nats.connect(builder.build()));
    }

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

    @NotNull
    public Connection getConnection() {
        return connection;
    }

    @NotNull
    public Dispatcher getDispatcher() {
        return dispatcher;
    }
}
