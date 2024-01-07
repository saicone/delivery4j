package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public abstract class AbstractMessenger implements DeliveryService {

    protected boolean checkDuplicated;

    protected DeliveryClient deliveryClient;
    protected final Map<String, Set<Consumer<String[]>>> incomingConsumers = new HashMap<>();
    protected Map<Integer, Long> cachedIds;

    public AbstractMessenger() {
        this(true);
    }

    public AbstractMessenger(boolean checkDuplicated) {
        this.checkDuplicated = checkDuplicated;
    }

    public boolean isCheckDuplicated() {
        return checkDuplicated;
    }

    public boolean isEnabled() {
        return deliveryClient != null && deliveryClient.isEnabled();
    }

    @Nullable
    public DeliveryClient getDeliveryClient() {
        return deliveryClient;
    }

    @NotNull
    public Set<String> getSubscribedChannels() {
        return incomingConsumers.keySet();
    }

    @NotNull
    public Map<String, Set<Consumer<String[]>>> getIncomingConsumers() {
        return incomingConsumers;
    }

    public void setCheckDuplicated(boolean checkDuplicated) {
        this.checkDuplicated = checkDuplicated;
    }

    @NotNull
    protected abstract DeliveryClient loadDeliveryClient();

    public void start() {
        start(loadDeliveryClient());
    }

    public void start(@NotNull DeliveryClient deliveryClient) {
        close();

        deliveryClient.getSubscribedChannels().addAll(getSubscribedChannels());
        deliveryClient.setService(this);

        if (this.checkDuplicated && this.cachedIds == null) {
            this.cachedIds = new HashMap<>();
        }

        this.deliveryClient = deliveryClient;
        this.deliveryClient.start();
    }

    public void close() {
        if (deliveryClient != null) {
            deliveryClient.close();
        }
    }

    public void clear() {
        if (deliveryClient != null) {
            deliveryClient.clear();
        }
        incomingConsumers.clear();
        if (cachedIds != null) {
            cachedIds.clear();
        }
    }

    public void subscribe(@NotNull String channel, @NotNull Consumer<String[]> incomingConsumer) {
        if (!incomingConsumers.containsKey(channel)) {
            incomingConsumers.put(channel, new HashSet<>());
        }
        incomingConsumers.get(channel).add(incomingConsumer);
        if (deliveryClient != null) {
            deliveryClient.subscribe(channel);
        }
    }

    public boolean send(@NotNull String channel, @Nullable Object... lines) {
        if (!isEnabled()) {
            return false;
        }

        try (ByteArrayOutputStream arrayOut = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(arrayOut)) {
            if (checkDuplicated) {
                final int id = createId();
                cacheAdd(id);
                out.writeInt(id);
            }
            out.writeInt(lines.length);
            for (Object message : lines) {
                out.writeUTF(Objects.toString(message));
            }
            deliveryClient.send(channel, arrayOut.toByteArray());
            return true;
        } catch (IOException e) {
            log(2, e);
            return false;
        }
    }

    @Override
    public boolean receive(@NotNull String channel, byte[] bytes) {
        final Set<Consumer<String[]>> consumers = incomingConsumers.get(channel);
        if (consumers == null || consumers.isEmpty()) {
            return false;
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (checkDuplicated && cacheContains(in.readInt())) {
                return false;
            }
            final String[] lines = new String[in.readInt()];
            try {
                for (int i = 0; i < lines.length; i++) {
                    final String message = in.readUTF();
                    lines[i] = message.equalsIgnoreCase("null") ? null : message;
                }
            } catch (EOFException ignored) { }
            for (Consumer<String[]> consumer : consumers) {
                consumer.accept(lines);
            }
            return true;
        } catch (IOException e) {
            log(2, e);
            return false;
        }
    }

    protected int createId() {
        return ThreadLocalRandom.current().nextInt(0, 999999 + 1);
    }

    protected void cacheAdd(int id) {
        cachedIds.put(id, System.currentTimeMillis() + 10000L);
    }

    protected boolean cacheContains(int id) {
        final long time = System.currentTimeMillis();
        cachedIds.entrySet().removeIf(entry -> entry.getValue() - time >= 10000);
        return cachedIds.containsKey(id);
    }

}