package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.util.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariBroker extends SqlBroker {

    private final HikariDataSource hikari;

    @NotNull
    public static HikariBroker of(@NotNull String url, @NotNull String user, @NotNull String password) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        return new HikariBroker(new HikariDataSource(config));
    }

    public HikariBroker(@NotNull HikariDataSource hikari) {
        super(new DataSource() {
            @Override
            public boolean isRunning() {
                return hikari.isRunning();
            }

            @Override
            public boolean isClosable() {
                return true;
            }

            @Override
            public @NotNull Connection getConnection() throws SQLException {
                return hikari.getConnection();
            }

            @Override
            public void close() {
                hikari.close();
            }
        });
        this.hikari = hikari;
    }

    @NotNull
    public HikariDataSource getHikari() {
        return hikari;
    }
}
