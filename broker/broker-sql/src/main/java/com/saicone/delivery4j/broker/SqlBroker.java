package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.saicone.delivery4j.util.DataSource;
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
 * Sql integration for data delivery using Hikari library.
 *
 * @author Rubenicos
 */
public class SqlBroker extends Broker {

    private final DataSource source;

    private String tablePrefix;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long currentID = -1;
    private Object getTask = null;
    private Object cleanTask = null;

    @NotNull
    public static SqlBroker of(@NotNull String url, @NotNull String user, @NotNull String password) throws SQLException {
        return new SqlBroker(DataSource.java(url, user, password));
    }

    public SqlBroker(@NotNull Connection con) {
        this(DataSource.java(con));
    }

    public SqlBroker(@NotNull DataSource source) {
        this.source = source;
    }

    @Override
    protected void onStart() {
        Connection connection = null;
        try {
            connection = this.source.getConnection();
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
                this.getTask = getExecutor().execute(this::getMessages, 10, 10, TimeUnit.SECONDS);
            }
            if (this.cleanTask == null) {
                this.cleanTask = getExecutor().execute(this::cleanMessages, 30, 30, TimeUnit.SECONDS);
            }
        } catch (SQLException e) {
            getLogger().log(1, "Cannot start sql connection", e);
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

    @Override
    protected void onClose() {
        try {
            this.source.close();
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
    protected void onSend(@NotNull String channel, byte[] data) throws IOException {
        if (!isEnabled()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.source.getConnection();
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO `" + this.tablePrefix + "messenger` (`time`, `channel`, `msg`) VALUES(NOW(), ?, ?)")) {
                statement.setString(1, channel);
                statement.setString(2, getCodec().encode(data));
                statement.execute();
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
        this.lock.readLock().unlock();
    }

    public void setTablePrefix(@NotNull String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    @NotNull
    public DataSource getSource() {
        return source;
    }

    @NotNull
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * Get all unread messages from database.
     */
    public void getMessages() {
        if (!isEnabled() || !this.source.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.source.getConnection();
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
                                receive(channel, getCodec().decode(message));
                            } catch (IOException e) {
                                getLogger().log(2, "Cannot process received message from channel '" + channel + "'", e);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            getLogger().log(2, "Cannot get messages from SQL database", e);
        } finally {
            if (connection != null && this.source.isClosable()) {
                try {
                    this.source.close();
                } catch (SQLException e) {
                    getLogger().log(2, "Cannot close sql connection", e);
                }
            }
        }
        this.lock.readLock().unlock();
    }

    /**
     * Clean old messages from database.
     */
    public void cleanMessages() {
        if (!isEnabled() || !this.source.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        Connection connection = null;
        try {
            connection = this.source.getConnection();
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM `" + this.tablePrefix + "messenger` WHERE (NOW() - `time` > 60)")) {
                statement.execute();
            }
        } catch (SQLException e) {
            getLogger().log(2, "Cannot clean old messages from SQL database", e);
        } finally {
            if (connection != null && this.source.isClosable()) {
                try {
                    this.source.close();
                } catch (SQLException e) {
                    getLogger().log(2, "Cannot close sql connection", e);
                }
            }
        }
        this.lock.readLock().unlock();
    }
}
