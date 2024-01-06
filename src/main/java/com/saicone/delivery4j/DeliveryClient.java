package com.saicone.delivery4j;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class DeliveryClient {

    protected DeliveryService service;

    protected final Set<String> subscribedChannels = new HashSet<>();
    protected boolean enabled = false;

    public void onStart() {
    }

    public void onClose() {
    }

    public void onSubscribe(@NotNull String... channels) {
    }

    public void onUnsubscribe(@NotNull String... channels) {
    }

    public void onSend(@NotNull String channel, byte[] data) {
    }

    public void onReceive(@NotNull String channel, byte[] data) {
    }

    public DeliveryService getService() {
        return service;
    }

    public Set<String> getSubscribedChannels() {
        return subscribedChannels;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setService(@NotNull DeliveryService service) {
        this.service = service;
    }

    @NotNull
    @Contract("_ -> this")
    public DeliveryClient setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public void start() {
        close();
        onStart();
    }

    public void close() {
        if (isEnabled()) {
            onClose();
        }
        enabled = false;
    }

    public void clear() {
        subscribedChannels.clear();
    }

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

    public void send(@NotNull String channel, byte[] data) {
        onSend(channel, data);
    }

    public void receive(@NotNull String channel, byte[] data) {
        if (service != null) {
            service.receive(channel, data);
        }
        onReceive(channel, data);
    }

    protected void log(@NotNull Throwable t) {
        if (service != null) {
            service.log(t);
        }
    }

    protected void log(int level, @NotNull String msg) {
        if (service != null) {
            service.log(level, msg);
        }
    }

    @NotNull
    protected Consumer<Boolean> async(@NotNull Runnable runnable) {
        if (service == null) {
            throw new IllegalStateException("Cannot execute async operation without delivery service");
        }
        return service.async(runnable);
    }

    @NotNull
    protected Consumer<Boolean> asyncRepeating(@NotNull Runnable runnable, long time, @NotNull TimeUnit unit) {
        if (service == null) {
            throw new IllegalStateException("Cannot execute async operation without delivery service");
        }
        return service.asyncRepeating(runnable, time, unit);
    }

    @NotNull
    protected String toBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    protected byte[] fromBase64(@NotNull String s) {
        return Base64.getDecoder().decode(s);
    }

}
