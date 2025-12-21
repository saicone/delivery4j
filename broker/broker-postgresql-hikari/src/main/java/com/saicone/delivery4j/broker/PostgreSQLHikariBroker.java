package com.saicone.delivery4j.broker;

import com.saicone.delivery4j.util.ConnectionSupplier;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * PostgreSQL broker implementation that use hikari library to make connections to database.<br>
 * The operations are the same as PostgreSQL broker, it just made any connection with hikari library.
 *
 * @author Rubenicos
 */
public class PostgreSQLHikariBroker extends PostgreSQLBroker {

    private final HikariDataSource hikari;

    /**
     * Create a postgres hikari broker with the provided connection parameters.
     *
     * @param url      the database url.
     * @param user     the database user on whose behalf the connection is being made.
     * @param password the user's password.
     * @return         a newly generated hikari broker containing the connection.
     */
    @NotNull
    public static PostgreSQLHikariBroker of(@NotNull String url, @NotNull String user, @NotNull String password) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        return new PostgreSQLHikariBroker(new HikariDataSource(config));
    }

    /**
     * Constructs a postgres hikari broker using the provided data source instance.
     *
     * @param hikari the data source that will be wrapped as cancellable data source.
     */
    public PostgreSQLHikariBroker(@NotNull HikariDataSource hikari) {
        super(new ConnectionSupplier() {
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

    /**
     * Get the current hikari data source used on this instance.
     *
     * @return a hikari data source.
     */
    @NotNull
    public HikariDataSource getHikari() {
        return hikari;
    }
}
