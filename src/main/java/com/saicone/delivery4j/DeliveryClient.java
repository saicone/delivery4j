package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Delivery client abstract class with common methods to transfer data and initialize any connection.
 *
 * @author Rubenicos
 */
public abstract class DeliveryClient {

    /**
     * The current delivery service.
     */
    protected DeliveryService service;

    /**
     * A Set of subscribed channels IDs.
     */
    protected final Set<String> subscribedChannels = new HashSet<>();
    /**
     * Boolean object to mark the current delivery client as enabled or disabled.
     */
    protected boolean enabled = false;

    /**
     * Method to run when client starts
     */
    public void onStart() {
    }

    /**
     * Method to run when client stops.
     */
    public void onClose() {
    }

    /**
     * Method to run when client is subscribed to new channels.
     *
     * @param channels the channels IDs.
     */
    public void onSubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when client is unsubscribed to new channels.
     *
     * @param channels the channels IDs.
     */
    public void onUnsubscribe(@NotNull String... channels) {
    }

    /**
     * Method to run when byte data is being sent to client.
     *
     * @param channel the channel ID.
     * @param data    the byte array data to send.
     */
    public void onSend(@NotNull String channel, byte[] data) {
    }

    /**
     * Method to run when byte data was received from client.
     *
     * @param channel the channel ID.
     * @param data    the received byte array data.
     */
    public void onReceive(@NotNull String channel, byte[] data) {
    }

    /**
     * Get the current delivery service.
     *
     * @return a registered delivery service.
     */
    public DeliveryService getService() {
        return service;
    }

    /**
     * Get the subscribed channels.
     *
     * @return a Set of channels IDs.
     */
    public Set<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    /**
     * Get the current client status.
     *
     * @return true if the client is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set a delivery service into client.
     *
     * @param service the delivery service to register.
     */
    public void setService(@NotNull DeliveryService service) {
        this.service = service;
    }

    /**
     * Set client status.
     *
     * @param enabled true for enabled, false otherwise.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Start the delivery client.
     */
    public void start() {
        close();
        onStart();
    }

    /**
     * Stop the delivery client.
     */
    public void close() {
        if (isEnabled()) {
            onClose();
        }
        enabled = false;
    }

    /**
     * Clear all subscribed channels from delivery client.
     */
    public void clear() {
        subscribedChannels.clear();
    }

    /**
     * Subscribe client into provided channels IDs.
     *
     * @param channels the channels to register.
     * @return         true if any channel was added, false if all the provided channels are already registered.
     */
    public boolean subscribe(@NotNull String... channels) {
        final List<String> list = new ArrayList<>();
        for (String channel : channels) {
            if (subscribedChannels.add(channel)) {
                list.add(channel);
            }
        }
        if (list.isEmpty()) {
            return false;
        }
        onSubscribe(list.toArray(new String[0]));
        return true;
    }

    /**
     * Unsubscribe client from provided channels IDs.
     *
     * @param channels the channels to unregister.
     * @return         true if any channel was removed, false if all the provided channels are already unregistered.
     */
    public boolean unsubscribe(@NotNull String... channels) {
        final List<String> list = new ArrayList<>();
        for (String channel : channels) {
            if (subscribedChannels.remove(channel)) {
                list.add(channel);
            }
        }
        if (list.isEmpty()) {
            return false;
        }
        onUnsubscribe(list.toArray(new String[0]));
        return true;
    }

    /**
     * Send byte data array to provided channel.
     *
     * @param channel the channel ID.
     * @param data    the data to send.
     */
    public void send(@NotNull String channel, byte[] data) {
        onSend(channel, data);
    }

    /**
     * Receive byte array from provided channel.
     *
     * @param channel the channel ID.
     * @param data    the data to receive.
     */
    public void receive(@NotNull String channel, byte[] data) {
        if (service != null) {
            service.receive(channel, data);
        }
        onReceive(channel, data);
    }

    /**
     * Log exception into delivery service.
     *
     * @see DeliveryService#log(int, Throwable)
     *
     * @param level the logging level.
     * @param t     the exception to log.
     */
    protected void log(int level, @NotNull Throwable t) {
        if (service != null) {
            service.log(level, t);
        }
    }

    /**
     * Log message into delivery service.
     *
     * @see DeliveryService#log(int, String)
     *
     * @param level the logging level.
     * @param msg   the message to log.
     */
    protected void log(int level, @NotNull String msg) {
        if (service != null) {
            service.log(level, msg);
        }
    }

    /**
     * Run task asynchronously into delivery service.
     *
     * @see DeliveryService#async(Runnable)
     *
     * @param runnable the task to run.
     * @return         a {@link Runnable} object to stop the executed task.
     * @throws IllegalStateException if there's no delivery service registered.
     */
    @NotNull
    protected Runnable async(@NotNull Runnable runnable) throws IllegalStateException {
        if (service == null) {
            throw new IllegalStateException("Cannot execute async operation without delivery service");
        }
        return service.async(runnable);
    }

    /**
     * Repeat task asynchronously into delivery service.
     *
     * @see DeliveryService#asyncRepeating(Runnable, long, TimeUnit)
     *
     * @param runnable the task to run.
     * @param time     the time interval.
     * @param unit     the unit of interval.
     * @return         a {@link Runnable} object to stop the executed task.
     * @throws IllegalStateException if there's no delivery service registered.
     */
    @NotNull
    protected Runnable asyncRepeating(@NotNull Runnable runnable, long time, @NotNull TimeUnit unit) throws IllegalStateException {
        if (service == null) {
            throw new IllegalStateException("Cannot execute async operation without delivery service");
        }
        return service.asyncRepeating(runnable, time, unit);
    }

    /**
     * Convert byte array to base64 string.
     *
     * @param data the byte array to encode.
     * @return     a String containing the resulting Base64 encoded characters.
     */
    @NotNull
    protected String toBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    /**
     * Convert base64 String to byte array.
     *
     * @param s the string to decode.
     * @return  a byte array containing the decoded bytes.
     */
    protected byte[] fromBase64(@NotNull String s) {
        return Base64.getDecoder().decode(s);
    }

}
