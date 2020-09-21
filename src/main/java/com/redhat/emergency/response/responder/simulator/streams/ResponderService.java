package com.redhat.emergency.response.responder.simulator.streams;

import java.time.Duration;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderService {

    private static final Logger log = LoggerFactory.getLogger(ResponderService.class);

    @Inject
    KafkaStreams streams;

    @ConfigProperty(name = "infinispan.streams.store", defaultValue = "responder-store")
    String storeName;

    public Uni<JsonObject> responder(String id) {
        return Uni.createFrom().item(() -> doGetResponder(id))
                .onFailure(ResponderNotFoundException.class).retry().withBackOff(Duration.ofMillis(500), Duration.ofMillis(1000)).atMost(10)
                .onFailure().recoverWithItem((Supplier<JsonObject>) JsonObject::new);
    }

    private JsonObject doGetResponder(String id) {
        String responderStr = responderStore().get(id);
        if (responderStr == null) {
            log.warn("Responder with id " + id + " not found in the responder store");
            throw new ResponderNotFoundException();
        }
        return new JsonObject(responderStr);
    }

    private ReadOnlyKeyValueStore<String, String> responderStore() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }

}
