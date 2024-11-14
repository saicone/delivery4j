package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Data source interface which provides a {@link Connection} interaction, depending on its implementation.
 *
 * @author Rubenicos
 */
public interface DataSource {

    /**
     * Create a data source using the provided connection parameters.
     *
     * @param url      the database url.
     * @param user     the database user on whose behalf the connection is being made.
     * @param password the user's password.
     * @return         a data source instance containing the newly created connection.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    static DataSource java(@NotNull String url, @Nullable String user, @Nullable String password) throws SQLException {
        return java(DriverManager.getConnection(url, user, password));
    }

    /**
     * Get a wrapped data source instance with the provided connection.<br>
     * This method assumes that the given connection should not be closed after return it.
     *
     * @param con the connection that will be offered by the instance.
     * @return    a data source instance containing the provided connection.
     */
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

    /**
     * Get the current running status from data source.
     *
     * @return true if it's available to give a connection.
     */
    default boolean isRunning() {
        return true;
    }

    /**
     * Get the current connection type offered by this data source.
     *
     * @return true if the connection should be closed after use it, false otherwise.
     */
    default boolean isClosable() {
        return false;
    }

    /**
     * Get the connection object from this data source.
     *
     * @return a connection that should be closed or not.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    Connection getConnection() throws SQLException;

    /**
     * Close the current data source, making it unable to give a connection after this operation.
     *
     * @throws SQLException if a database access error occurs.
     */
    void close() throws SQLException;
}
