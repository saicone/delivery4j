package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.DataSource;
import org.jetbrains.annotations.NotNull;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class PostgreSQLBroker extends Broker {

    private final DataSource source;

    private int timeout = 5000;
    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    private Connection connection;
    private PGConnection pgConnection;

    private Object listenTask;
    private boolean reconnected = false;

    @NotNull
    public static PostgreSQLBroker of(@NotNull String url, @NotNull String user, @NotNull String password) throws SQLException {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        return new PostgreSQLBroker(dataSource.getConnection());
    }

    public PostgreSQLBroker(@NotNull Connection con) {
        this(DataSource.java(con));
    }

    public PostgreSQLBroker(@NotNull DataSource source) {
        this.source = source;
    }

    @Override
    protected void onStart() {
        try {
            this.connection = this.source.getConnection();
            if (!getSubscribedChannels().isEmpty()) {
                try (Statement stmt = this.connection.createStatement()) {
                    for (String channel : getSubscribedChannels()) {
                        stmt.execute("LISTEN " + channel);
                    }
                }
            }

            this.pgConnection = this.connection.unwrap(PGConnection.class);

            if (this.reconnected) {
                getLogger().log(3, "PostgreSQL connection is alive again");
            }
            setEnabled(true);
        } catch (SQLException e) {
            getLogger().log(1, "Cannot start postgres connection", e);

            // Make an instant reconnection if occurs any error on initialization
            if (!this.reconnected) {
                this.reconnected = true;
                onStart();
                return;
            }
        }

        if (isEnabled()) {
            this.listenTask = getExecutor().execute(this::listen);
        }
    }

    @Override
    protected void onClose() {
        try {
            this.source.close();
        } catch (SQLException ignored) { }
        disconnect();
    }

    @Override
    protected void onSubscribe(@NotNull String... channels) {
        if (this.connection == null) {
            return;
        }
        try (Statement stmt = this.connection.createStatement()) {
            for (@NotNull String channel : channels) {
                stmt.execute("LISTEN " + channel);
            }
        } catch (SQLException e) {
            getLogger().log(2, "Cannot subscribe to channel", e);
        }
    }

    @Override
    protected void onUnsubscribe(@NotNull String... channels) {
        if (this.connection == null) {
            return;
        }
        try (Statement stmt = this.connection.createStatement()) {
            for (@NotNull String channel : channels) {
                stmt.execute("UNLISTEN " + channel);
            }
        } catch (SQLException e) {
            getLogger().log(2, "Cannot subscribe to channel", e);
        }
    }

    @Override
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        Connection connection = null;
        try {
            connection = this.source.getConnection();
            // Using SELECT due NOTIFY is not compatible with prepared statements
            try (PreparedStatement stmt = connection.prepareStatement("SELECT pg_notify(?, ?)")) {
                stmt.setString(1, channel);
                stmt.setString(2, getCodec().encode(data));
                stmt.execute();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (connection != null && this.source.isClosable()) {
                try {
                    this.source.close();
                } catch (SQLException e) {
                    getLogger().log(2, "Cannot close sql connection", e);
                }
            }
        }
    }

    public void setTimeout(long time, @NotNull TimeUnit unit) {
        this.timeout = (int) unit.toMillis(time);
    }

    public void setReconnectionInterval(int time, @NotNull TimeUnit unit) {
        this.sleepTime = time;
        this.sleepUnit = unit;
    }

    @NotNull
    public DataSource getSource() {
        return source;
    }

    private void listen() {
        try {
            while (isEnabled() && !Thread.interrupted()) {
                final PGNotification[] notifications = this.pgConnection.getNotifications(this.timeout);
                if (notifications == null) {
                    continue;
                }
                for (PGNotification notification : notifications) {
                    final String channel = notification.getName();
                    if (getSubscribedChannels().contains(channel)) {
                        try {
                            receive(channel, getCodec().decode(notification.getParameter()));
                        } catch (Throwable t) {
                            getLogger().log(2, "Cannot process received message from channel '" + channel + "'", t);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (!isEnabled()) {
                return;
            }
            disconnect();
            setEnabled(false);
            this.reconnected = true;
            getLogger().log(2, () -> "PostgreSQL connection dropped, automatic reconnection every " + this.sleepTime + " " + this.sleepUnit.name().toLowerCase() + "...", t);
            try {
                Thread.sleep(this.sleepUnit.toMillis(this.sleepTime));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            onStart();
        }
    }

    private void disconnect() {
        if (this.connection != null) {
            if (this.source.isClosable()) {
                try {
                    this.connection.close();
                } catch (SQLException ignored) { }
            }
            this.connection = null;
            this.pgConnection = null;
        }
        if (this.listenTask != null) {
            getExecutor().cancel(this.listenTask);
            this.listenTask = null;
        }
    }
}
