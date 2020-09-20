package com.redhat.emergency.response.responder.simulator.streams.infinispan;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

@ApplicationScoped
public class InfinispanKeyValueStoreSupplier implements KeyValueBytesStoreSupplier {

    @Inject
    InfinispanKeyValueStore keyValueStore;

    @Override
    public String name() {
        return keyValueStore.name();
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return keyValueStore;
    }

    @Override
    public String metricsScope() {
        return keyValueStore.name();
    }
}
