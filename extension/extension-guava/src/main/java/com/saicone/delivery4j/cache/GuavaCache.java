package com.saicone.delivery4j.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.saicone.delivery4j.MessageChannel;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

public class GuavaCache extends MessageChannel.Cache {

    private static final Object DUMMY = new Object();

    private final Cache<Integer, Object> cache;

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