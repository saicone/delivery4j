package com.saicone.delivery4j.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Minimalist interface which provides a {@link Connection}.<br>
 * This implementation can also provide information about connection state.
 *
 * @author Rubenicos
 */
public interface ConnectionSupplier extends Supplier<Connection> {

    /**
     * Create a connection supplier using the provided connection parameters.
     *
     * @param url      the database url.
     * @param user     the database user on whose behalf the connection is being made.
     * @param password the user's password.
     * @return         a newly generated connection supplier containing the created connection.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    static ConnectionSupplier valueOf(@NotNull String url, @Nullable String user, @Nullable String password) throws SQLException {
        return valueOf(DriverManager.getConnection(url, user, password));
    }

    /**
     * Get a wrapped connection supplier instance with the provided connection.<br>
     * This method assumes that the given connection should not be closed after return it.
     *
     * @param con the connection that will be offered by the instance.
     * @return    a newly generated connection supplier containing the provided connection.
     */
    @NotNull
    static ConnectionSupplier valueOf(@NotNull Connection con) {
        return new ConnectionSupplier() {
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
     * Get the current running status connection.
     *
     * @return true if it's available to give a connection value.
     */
    default boolean isRunning() {
        return true;
    }

    /**
     * Get the current connection type offered by this supplier.
     *
     * @return true if the connection should be closed after use it, false otherwise.
     */
    default boolean isClosable() {
        return false;
    }

    @Override
    default Connection get() {
        try {
            return getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the connection object from this supplier.
     *
     * @return a connection that should be closed or not.
     * @throws SQLException if a database access error occurs.
     */
    @NotNull
    Connection getConnection() throws SQLException;

    /**
     * Close the provided connection, making it unable to give a value after this operation.
     *
     * @throws SQLException if a database access error occurs.
     */
    void close() throws SQLException;
}
