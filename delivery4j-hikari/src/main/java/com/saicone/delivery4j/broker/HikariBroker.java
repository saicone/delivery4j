package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.Broker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
public class HikariBroker extends Broker<HikariBroker> {

    private final HikariDataSource hikari;
    private final String tablePrefix;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long currentID = -1;
    private Object getTask = null;
    private Object cleanTask = null;

    /**
     * Create a HikariDelivery client with provided parameters.
     *
     * @param url         the URL to connect with.
     * @param username    the username to validate authentication.
     * @param password    the password to validate authentication.
     * @param tablePrefix the table prefix.
     * @return            new HikariDelivery instance.
     */
    @NotNull
    public static HikariBroker of(@NotNull String url, @NotNull String username, @NotNull String password, @NotNull String tablePrefix) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        return new HikariBroker(new HikariDataSource(config), tablePrefix);
    }

    /**
     * Constructs a HikariDelivery with provided parameters.
     *
     * @param hikari      the hikari instance to make database connections.
     * @param tablePrefix the used table prefix.
     */
    public HikariBroker(@NotNull HikariDataSource hikari, @NotNull String tablePrefix) {
        this.hikari = hikari;
        this.tablePrefix = tablePrefix;
    }

    @Override
    protected void onStart() {
        try (Connection connection = this.hikari.getConnection()) {
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
            getLogger().log(1, "Cannot start hikari connection", e);
        }
    }

    @Override
    protected void onClose() {
        this.hikari.close();
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

        try (Connection connection = this.hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO `" + this.tablePrefix + "messenger` (`time`, `channel`, `msg`) VALUES(NOW(), ?, ?)")) {
                statement.setString(1, channel);
                statement.setString(2, getCodec().encode(data));
                statement.execute();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        this.lock.readLock().unlock();
    }

    @Override
    protected @NotNull HikariBroker get() {
        return this;
    }

    /**
     * Get the current hikari instance.
     *
     * @return a hikari instance.
     */
    @NotNull
    public HikariDataSource getHikari() {
        return hikari;
    }

    /**
     * Get all unread messages from database.
     */
    public void getMessages() {
        if (!isEnabled() || !this.hikari.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        try (Connection connection = this.hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT `id`, `channel`, `msg` FROM `" + this.tablePrefix + "messenger` WHERE `id` > ? AND (NOW() - `time` < 30)")) {
                statement.setLong(1, this.currentID);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        this.currentID = Math.max(this.currentID, id);

                        String channel = rs.getString("channel");
                        String message = rs.getString("msg");
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
        }
        this.lock.readLock().unlock();
    }

    /**
     * Clean old messages from database.
     */
    public void cleanMessages() {
        if (!isEnabled() || !this.hikari.isRunning()) {
            return;
        }
        this.lock.readLock().lock();

        try (Connection connection = this.hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM `" + this.tablePrefix + "messenger` WHERE (NOW() - `time` > 60)")) {
                statement.execute();
            }
        } catch (SQLException e) {
            getLogger().log(2, "Cannot clean old messages from SQL database", e);
        }
        this.lock.readLock().unlock();
    }
}
