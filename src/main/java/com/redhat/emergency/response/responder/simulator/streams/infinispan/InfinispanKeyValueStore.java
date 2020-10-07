package com.redhat.emergency.response.responder.simulator.streams.infinispan;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.redhat.emergency.response.responder.simulator.infinispan.Configuration;
import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class InfinispanKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger log = LoggerFactory.getLogger(InfinispanKeyValueStore.class);

    private volatile boolean open = false;

    @Inject
    RemoteCacheManager cacheManager;

    @ConfigProperty(name = "infinispan.streams.store", defaultValue = "responder-store")
    String storeName;

    @ConfigProperty(name = "infinispan.cache.create.lazy", defaultValue = "false")
    boolean lazy;

    volatile RemoteCache<byte[], byte[]> cache;

    void onStart(@Observes StartupEvent e) {
        // do not initialize the cache at startup when remote cache is not available, e.g. in QuarkusTests
        if (!lazy) {
            cache = initCache();
        }
    }

    @Override
    public void put(Bytes key, byte[] value) {
        if (value == null) {
            delete(key);
        } else {
            getCache().putIfAbsent(key.get(), value);
        }
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        return getCache().putIfAbsent(key.get(), value);
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        if (entries == null) {
            return;
        }
        Map<byte[], byte[]> keyValueMap = entries.stream().collect(Collectors.toMap(k -> k.key.get(), v -> v.value));
        getCache().putAll(keyValueMap);
    }

    @Override
    public byte[] delete(Bytes key) {
        return getCache().remove(key.get());
    }

    @Override
    public String name() {
        return storeName;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        if (root != null) {
            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
        }
        open = true;
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public byte[] get(Bytes key) {
        return getCache().get(key.get());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        throw new UnsupportedOperationException("Method range not implemented)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("Method all not implemented)");
    }

    @Override
    public long approximateNumEntries() {
        return getCache().size();
    }

    private RemoteCache<byte[], byte[]> getCache() {
        RemoteCache<byte[], byte[]> cache = this.cache;
        if (cache == null) {
            synchronized(this) {
                if (this.cache == null) {
                    this.cache = cache = initCache();
                }
            }
        }
        return cache;
    }

    private RemoteCache<byte[], byte[]> initCache() {
        log.info("Creating remote cache '" + storeName + "'");
        Configuration configuration = Configuration.builder().name(storeName).mode("SYNC").owners(2).build();
        return cacheManager.administration().getOrCreateCache(storeName, configuration);
    }
}
