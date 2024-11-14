package com.saicone.delivery4j.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.saicone.delivery4j.MessageChannel;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Guava integration for message ID caching.
 *
 * @author Rubenicos
 */
public class GuavaCache extends MessageChannel.Cache {

    private static final Object DUMMY = new Object();

    private final Cache<Integer, Object> cache;

    /**
     * Constructs a guava cache with provided expiration.
     *
     * @param duration the length of time after a message ID is automatically removed.
     * @param unit     the unit that {@code duration} is expressed in.
     */
    public GuavaCache(long duration, @NotNull TimeUnit unit) {
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(duration, unit).build();
    }

    @Override
    protected void save(int id) {
        this.cache.put(id, DUMMY);
    }

    @Override
    public boolean contains(int id) {
        return this.cache.getIfPresent(id) != null;
    }

    @Override
    public void clear() {
        this.cache.invalidateAll();
    }
}