package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.DataSource;
import com.saicone.delivery4j.util.LogFilter;
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

/**
 * PostgreSQL broker implementation to send data using {@code LISTEN} and {@code NOTIFY} statements.<br>
 * <br>
 * <b>Is this scalable?</b><br>
 * No<br>
 * <b>Allows large messages?</b><br>
 * No<br>
 * <b>Should I use it?</b><br>
 * Maybe
 *
 * @author Rubenicos
 */
public class PostgreSQLBroker extends Broker {

    private final DataSource source;

    private int timeout = 5000;
    private long sleepTime = 8;
    private TimeUnit sleepUnit = TimeUnit.SECONDS;

    private Connection connection;
    private PGConnection pgConnection;

    private Object listenTask;
    private boolean reconnected = false;

    /**
     * Create a postgres broker with the provided connection parameters.
     *
     * @param url      the database url.
     * @param user     the database user on whose behalf the connection is being made.
     * @param password the user's password.
     * @return         a newly generated sql broker containing the connection.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    public static PostgreSQLBroker of(@NotNull String url, @NotNull String user, @NotNull String password) throws SQLException {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        return new PostgreSQLBroker(dataSource.getConnection());
    }

    /**
     * Constructs a postgres broker using the provided connection instance.<br>
     * This constructor assumes that the given connection should not be closed after return it.
     *
     * @param con the connection that will be wrapped as non-cancellable data source.
     */
    public PostgreSQLBroker(@NotNull Connection con) {
        this(DataSource.java(con));
    }

    /**
     * Constructs a postgres broker using the provided data source instance.
     *
     * @param source the data source that provide a database connection.
     */
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
                getLogger().log(LogFilter.INFO, "PostgreSQL connection is alive again");
            }
            setEnabled(true);
        } catch (SQLException e) {
            getLogger().log(LogFilter.ERROR, "Cannot start postgres connection", e);

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
            getLogger().log(LogFilter.WARNING, "Cannot subscribe to channel", e);
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
            getLogger().log(LogFilter.WARNING, "Cannot subscribe to channel", e);
        }
    }

    @Override
    public void send(@NotNull String channel, byte[] data) throws IOException {
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
                    getLogger().log(LogFilter.WARNING, "Cannot close sql connection", e);
                }
            }
        }
    }

    /**
     * Set timeout that will be used to listen for new notifications from database,
     * this is the maximum communication delay between applications.<br>
     * By default, 5 seconds is used.
     *
     * @param time the delay between listen execution.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setTimeout(long time, @NotNull TimeUnit unit) {
        this.timeout = (int) unit.toMillis(time);
    }

    /**
     * Set the reconnection interval that will be used on this postgres broker instance.<br>
     * By default, 8 seconds is used.
     *
     * @param time the time to wait until reconnection is performed.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setReconnectionInterval(int time, @NotNull TimeUnit unit) {
        this.sleepTime = time;
        this.sleepUnit = unit;
    }

    /**
     * Get the current data source that database connection is from.
     *
     * @return a data source connection.
     */
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
                            getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + channel + "'", t);
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
            getLogger().log(LogFilter.WARNING, () -> "PostgreSQL connection dropped, automatic reconnection every " + this.sleepTime + " " + this.sleepUnit.name().toLowerCase() + "...", t);
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
