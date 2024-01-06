package com.saicone.delivery4j.client;

import com.saicone.delivery4j.DeliveryClient;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class HikariDelivery extends DeliveryClient {

    private final HikariDataSource hikari;
    private final String tablePrefix;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long currentID = -1;
    private Consumer<Boolean> getTask = null;
    private Consumer<Boolean> cleanTask = null;

    @NotNull
    public static HikariDelivery of(@NotNull String url, @NotNull String username, @NotNull String password, @NotNull String tablePrefix) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        return new HikariDelivery(new HikariDataSource(config), tablePrefix);
    }

    public HikariDelivery(@NotNull HikariDataSource hikari, @NotNull String tablePrefix) {
        this.hikari = hikari;
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void onStart() {
        try (Connection connection = hikari.getConnection()) {
            String createTable = "CREATE TABLE IF NOT EXISTS `" + tablePrefix + "messenger` (`id` INT AUTO_INCREMENT NOT NULL, `time` TIMESTAMP NOT NULL, `channel` VARCHAR(255) NOT NULL, `msg` TEXT NOT NULL, PRIMARY KEY (`id`)) DEFAULT CHARSET = utf8mb4";
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

            try (PreparedStatement statement = connection.prepareStatement("SELECT MAX(`id`) as `latest` FROM `" + tablePrefix + "messenger`")) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        currentID = resultSet.getLong("latest");
                    }
                }
            }
            enabled = true;
            if (getTask == null) {
                getTask = asyncRepeating(this::getMessages, 10, TimeUnit.SECONDS);
            } else {
                getTask.accept(true);
            }
            if (cleanTask == null) {
                cleanTask = asyncRepeating(this::cleanMessages, 30, TimeUnit.SECONDS);
            } else {
                cleanTask.accept(true);
            }
        } catch (SQLException e) {
            log(e);
        }
    }

    @Override
    public void onClose() {
        hikari.close();
        if (getTask != null) {
            getTask.accept(false);
        }
        if (cleanTask != null) {
            cleanTask.accept(false);
        }
    }

    @Override
    public void onSend(@NotNull String channel, byte[] data) {
        if (!enabled) {
            return;
        }
        lock.readLock().lock();

        try (Connection connection = hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO `" + tablePrefix + "messenger` (`time`, `channel`, `msg`) VALUES(NOW(), ?, ?)")) {
                statement.setString(1, channel);
                statement.setString(2, toBase64(data));
                statement.execute();
            }
        } catch (SQLException e) {
            log(e);
        }
        lock.readLock().unlock();
    }

    @NotNull
    public HikariDataSource getHikari() {
        return hikari;
    }

    public void getMessages() {
        if (!enabled || !hikari.isRunning()) {
            return;
        }
        lock.readLock().lock();

        try (Connection connection = hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT `id`, `channel`, `msg` FROM `" + tablePrefix + "messenger` WHERE `id` > ? AND (NOW() - `time` < 30)")) {
                statement.setLong(1, currentID);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        currentID = Math.max(currentID, id);

                        String channel = rs.getString("channel");
                        String message = rs.getString("msg");
                        if (subscribedChannels.contains(channel) && message != null) {
                            receive(channel, fromBase64(message));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log(e);
        }
        lock.readLock().unlock();
    }

    public void cleanMessages() {
        if (!enabled || !hikari.isRunning()) {
            return;
        }
        lock.readLock().lock();

        try (Connection connection = hikari.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM `" + tablePrefix + "messenger` WHERE (NOW() - `time` > 60)")) {
                statement.execute();
            }
        } catch (SQLException e) {
            log(e);
        }
        lock.readLock().unlock();
    }
}
