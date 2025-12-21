package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.PlainTextBroker;
import com.saicone.delivery4j.util.ConnectionSupplier;
import com.saicone.delivery4j.util.LogFilter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sql broker implementation to send data via polling.<br>
 * Take in count that this is not a real broker, can be used as one
 * but the client is doing all the job via insertion and repeatable deletion.
 *
 * @author Rubenicos
 */
public class SqlBroker extends PlainTextBroker {

    private final ConnectionSupplier connectionSupplier;

    private String tablePrefix;
    private int pollTime = 10;
    private TimeUnit pollUnit = TimeUnit.SECONDS;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long currentID = -1;
    private Object getTask = null;
    private Object cleanTask = null;

    /**
     * Create a sql broker with the provided connection parameters.
     *
     * @param url      the database url.
     * @param user     the database user on whose behalf the connection is being made.
     * @param password the user's password.
     * @return         a newly generated sql broker containing the connection.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    public static SqlBroker of(@NotNull String url, @NotNull String user, @NotNull String password) throws SQLException {
        return new SqlBroker(ConnectionSupplier.valueOf(url, user, password));
    }

    /**
     * Constructs a sql broker using the provided connection instance.<br>
     * This constructor assumes that the given connection should not be closed after return it.
     *
     * @param con the connection that will be wrapped as non-cancellable connection supplier.
     */
    public SqlBroker(@NotNull Connection con) {
        this(ConnectionSupplier.valueOf(con));
    }

    /**
     * Constructs a sql broker using the provided connection supplier instance.
     *
     * @param connectionSupplier the supplier that provide a database connection.
     */
    public SqlBroker(@NotNull ConnectionSupplier connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    protected void onStart() {
        Connection connection = null;
        try {
            connection = this.connectionSupplier.getConnection();
            String createTable = "CREATE TABLE IF NOT EXISTS `" + this.tablePrefix + "messenger` (`id` INT AUTO_INCREMENT NOT NULL, `time` TIMESTAMP NOT NULL, `channel` VARCHAR(255) NOT NULL, `msg` TEXT NOT NULL, PRIMARY KEY (`id`)) DEFAULT CHARSET = utf8mb4";
            // Taken from LuckPerms
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute(createTable);
                } catch (SQLException e) {
                    if (e.getMessage().contains("Unknown character set")) {
                        statement.execute(createTable.replace("utf8mb4", "utf8"));
                    } else {
                        throw e;
                    }
                }
            }

            try (PreparedStatement statement = connection.prepareStatement("SELECT MAX(`id`) as `latest` FROM `" + this.tablePrefix + "messenger`")) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        currentID = resultSet.getLong("latest");
                    }
                }
            }
            setEnabled(true);
            if (this.getTask == null) {
                this.getTask = getExecutor().execute(this::getMessages, this.pollTime, this.pollTime, this.pollUnit);
            }
            if (this.cleanTask == null) {
                this.cleanTask = getExecutor().execute(this::cleanMessages, this.pollTime * 30L, this.pollTime * 30L, this.pollUnit);
            }
        } catch (SQLException e) {
            getLogger().log(LogFilter.ERROR, "Cannot start sql connection", e);
        } finally {
            if (connection != null && this.connectionSupplier.isClosable()) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    getLogger().log(LogFilter.WARNING, "Cannot close sql connection", e);
                }
            }
        }
    }

    @Override
    protected void onClose() {
        try {
            this.connectionSupplier.close();
        } catch (SQLException ignored) { }
        if (this.getTask != null) {
            getExecutor().cancel(this.getTask);
            this.getTask = null;
        }
        if (this.cleanTask != null) {
            getExecutor().cancel(this.cleanTask);
            this.cleanTask = null;
        }
    }

    @Override
    public void send(@NotNull String channel, @NotNull String data) throws IOException {
        if (!isEnabled()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.connectionSupplier.getConnection();
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO `" + this.tablePrefix + "messenger` (`time`, `channel`, `msg`) VALUES(NOW(), ?, ?)")) {
                statement.setString(1, channel);
                statement.setString(2, data);
                statement.execute();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (connection != null && this.connectionSupplier.isClosable()) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    getLogger().log(LogFilter.WARNING, "Cannot close sql connection", e);
                }
            }
        }
        this.lock.readLock().unlock();
    }

    /**
     * Set the table prefix that will be used before {@code messenger} table name.
     *
     * @param tablePrefix a table prefix.
     */
    public void setTablePrefix(@NotNull String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    /**
     * Set the poll that will be used to check for new messages from database,
     * this is the maximum communication delay between applications.<br>
     * Setting a different poll interval between applications may cause
     * synchronization errors due it's value is multiplied by 3 to delete old
     * messages from database in that interval.<br>
     * By default, 10 seconds is used.
     *
     * @param time the delay between database poll.
     * @param unit the unit that {@code time} is expressed in.
     */
    public void setPollInterval(int time, @NotNull TimeUnit unit) {
        this.pollTime = time;
        this.pollUnit = unit;
    }

    /**
     * Get the current connection supplier that database connection is from.
     *
     * @return a connection supplier connection.
     */
    @NotNull
    public ConnectionSupplier getConnectionSupplier() {
        return connectionSupplier;
    }

    /**
     * Get the current table prefix that is used before {@code messenger} table name.
     *
     * @return a table prefix.
     */
    @NotNull
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * Get all unread messages from database.
     */
    public void getMessages() {
        if (!isEnabled() || !this.connectionSupplier.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.connectionSupplier.getConnection();
            try (PreparedStatement statement = connection.prepareStatement("SELECT `id`, `channel`, `msg` FROM `" + this.tablePrefix + "messenger` WHERE `id` > ? AND (NOW() - `time` < 30)")) {
                statement.setLong(1, this.currentID);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        this.currentID = Math.max(this.currentID, id);

                        final String channel = rs.getString("channel");
                        final String message = rs.getString("msg");
                        if (getSubscribedChannels().contains(channel) && message != null) {
                            try {
                                receive(channel, message);
                            } catch (IOException e) {
                                getLogger().log(LogFilter.WARNING, "Cannot process received message from channel '" + channel + "'", e);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            getLogger().log(LogFilter.WARNING, "Cannot get messages from SQL database", e);
        } finally {
            if (connection != null && this.connectionSupplier.isClosable()) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    getLogger().log(LogFilter.WARNING, "Cannot close sql connection", e);
                }
            }
        }
        this.lock.readLock().unlock();
    }

    /**
     * Clean old messages from database.
     */
    public void cleanMessages() {
        if (!isEnabled() || !this.connectionSupplier.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.connectionSupplier.getConnection();
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM `" + this.tablePrefix + "messenger` WHERE (NOW() - `time` > 60)")) {
                statement.execute();
            }
        } catch (SQLException e) {
            getLogger().log(LogFilter.WARNING, "Cannot clean old messages from SQL database", e);
        } finally {
            if (connection != null && this.connectionSupplier.isClosable()) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    getLogger().log(LogFilter.WARNING, "Cannot close sql connection", e);
                }
            }
        }
        this.lock.readLock().unlock();
    }
}
