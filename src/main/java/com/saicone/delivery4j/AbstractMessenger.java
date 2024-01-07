package com.saicone.delivery4j;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Messenger abstract class to send messages across channels using a {@link DeliveryClient}.
 *
 * @author Rubenicos
 */
public abstract class AbstractMessenger implements DeliveryService {

    /**
     * Check or not messages sent by this instance.
     */
    protected boolean checkDuplicated;

    /**
     * Current delivery client.
     */
    protected DeliveryClient deliveryClient;
    /**
     * Registered incoming consumers by channel ID.
     */
    protected final Map<String, Set<Consumer<String[]>>> incomingConsumers = new HashMap<>();
    /**
     * Cached messages IDs sent by this instance.
     */
    protected Map<Integer, Long> cachedIds;

    /**
     * Constructs a new messenger using check duplicated option by default.
     */
    public AbstractMessenger() {
        this(true);
    }

    /**
     * Constructs a new messenger with specified parameters.
     *
     * @param checkDuplicated true to ignore messages sent by this instance.
     */
    public AbstractMessenger(boolean checkDuplicated) {
        this.checkDuplicated = checkDuplicated;
    }

    /**
     * Get check duplicated status.
     *
     * @return true if messenger instance is ignoring sent messages.
     */
    public boolean isCheckDuplicated() {
        return checkDuplicated;
    }

    /**
     * Get the current messenger status.
     *
     * @return true if the messenger is enabled.
     */
    public boolean isEnabled() {
        return deliveryClient != null && deliveryClient.isEnabled();
    }

    /**
     * Get the current delivery client.
     *
     * @return a delivery client or null.
     */
    @Nullable
    public DeliveryClient getDeliveryClient() {
        return deliveryClient;
    }

    /**
     * Get all the subscribed channels.
     *
     * @return a Set of channels IDs.
     */
    @NotNull
    public Set<String> getSubscribedChannels() {
        return incomingConsumers.keySet();
    }

    /**
     * Get the incoming consumers.
     *
     * @return a map of message consumers separated by channels IDs.
     */
    @NotNull
    public Map<String, Set<Consumer<String[]>>> getIncomingConsumers() {
        return incomingConsumers;
    }

    /**
     * Set check duplicated status.
     *
     * @param checkDuplicated true to ignore messages sent by this instance.
     */
    public void setCheckDuplicated(boolean checkDuplicated) {
        this.checkDuplicated = checkDuplicated;
    }

    /**
     * Method to load the used delivery client on data transfer operations.
     *
     * @return an usable delivery client.
     */
    @NotNull
    protected abstract DeliveryClient loadDeliveryClient();

    /**
     * Start the messenger instance.
     */
    public void start() {
        start(loadDeliveryClient());
    }

    /**
     * Start the messenger instance with a provided delivery client.
     *
     * @param deliveryClient the delivery client to use.
     */
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

    /**
     * Stop the messenger instance.
     */
    public void close() {
        if (deliveryClient != null) {
            deliveryClient.close();
        }
    }

    /**
     * Clear any delivery client channels and incoming consumers.
     */
    public void clear() {
        if (deliveryClient != null) {
            deliveryClient.clear();
        }
        incomingConsumers.clear();
        if (cachedIds != null) {
            cachedIds.clear();
        }
    }

    /**
     * Subscribe into a messaging channel by providing a consumer.
     *
     * @param channel          the channel ID.
     * @param incomingConsumer the consumer that accept multiple line message.
     */
    public void subscribe(@NotNull String channel, @NotNull Consumer<String[]> incomingConsumer) {
        if (!incomingConsumers.containsKey(channel)) {
            incomingConsumers.put(channel, new HashSet<>());
        }
        incomingConsumers.get(channel).add(incomingConsumer);
        if (deliveryClient != null) {
            deliveryClient.subscribe(channel);
        }
    }

    /**
     * Send message into channel.
     *
     * @param channel the channel ID.
     * @param lines   the message to send.
     * @return        true if the message was sent.
     */
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

    /**
     * Create int-based id to detect duplicated messages.
     *
     * @return a random number.
     */
    protected int createId() {
        return ThreadLocalRandom.current().nextInt(0, 999999 + 1);
    }

    /**
     * Add int id into cache to check later.
     *
     * @param id the number to save.
     */
    protected void cacheAdd(int id) {
        cachedIds.put(id, System.currentTimeMillis() + 10000L);
    }

    /**
     * Check if the int id is cached to ignore.
     *
     * @param id the number to check.
     * @return   true if the number is cached.
     */
    protected boolean cacheContains(int id) {
        final long time = System.currentTimeMillis();
        cachedIds.entrySet().removeIf(entry -> entry.getValue() - time >= 10000);
        return cachedIds.containsKey(id);
    }

}