package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public interface DataSource {

    @NotNull
    static DataSource java(@NotNull String url, @NotNull String user, @NotNull String password) throws SQLException {
        return java(DriverManager.getConnection(url, user, password));
    }

    @NotNull
    static DataSource java(@NotNull Connection con) {
        return new DataSource() {
            @Override
            public @NotNull Connection getConnection() {
                return con;
            }

            @Override
            public void close() throws SQLException {
                con.close();
            }
        };
    }

    default boolean isRunning() {
        return true;
    }

    default boolean isClosable() {
        return false;
    }

    @NotNull
    Connection getConnection() throws SQLException;

    void close() throws SQLException;
}
