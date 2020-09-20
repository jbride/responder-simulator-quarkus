package com.redhat.emergency.response.responder.simulator.streams;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.redhat.emergency.response.responder.simulator.streams.infinispan.InfinispanKeyValueStoreSupplier;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TopologyProducer {

    private static final Logger log = LoggerFactory.getLogger(TopologyProducer.class);

    private static final String RESPONDERS_CREATED_EVENT = "RespondersCreatedEvent";
    private static final String RESPONDERS_DELETED_EVENT = "RespondersDeletedEvent";
    private static final String[] ACCEPTED_MESSAGE_TYPES = {RESPONDERS_CREATED_EVENT, RESPONDERS_DELETED_EVENT};

    private static final String ACTION_CREATE = "create";
    private static final String ACTION_DELETE = "delete";


    @ConfigProperty(name = "kafka.topic.responder-event")
    String responderEventTopic;

    @ConfigProperty(name = "simulator.distance.base")
    double baseDistance;

    @ConfigProperty(name = "simulator.distance.variation")
    double distanceVariation;

    @Inject
    InfinispanKeyValueStoreSupplier keyValueStoreSupplier;

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(responderEventTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> {
                    try {
                        JsonObject json = new JsonObject(value);
                        log.debug("Processing message: " + value);
                        return json;
                    } catch (Exception e) {
                        log.warn("Unexpected message which is not a valid JSON object");
                        return new JsonObject();
                    }
                })
                .filter((key, value) -> {
                    String messageType = value.getString("messageType");
                    if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType)) {
                        return true;
                    }
                    log.debug("Message with type '" + messageType + "' is ignored");
                    return false;
                })
                .flatMapValues((ValueMapper<JsonObject, Iterable<JsonObject>>) value -> {
                    if (RESPONDERS_CREATED_EVENT.equals(value.getString("messageType"))) {
                        JsonArray responders = value.getJsonObject("body").getJsonArray("responders");
                        return responders.stream().map(o -> (JsonObject)o).map(resp -> new JsonObject().put("action", ACTION_CREATE)
                                .put("id", resp.getString("id"))
                                .put("responder", resp.put("distanceUnit", distanceUnit())))
                                .collect(Collectors.toList());
                    } else {
                        JsonArray responderIds = value.getJsonObject("body").getJsonArray("responders");
                        return responderIds.stream().map(id -> new JsonObject().put("action", ACTION_DELETE)
                                .put("id", id)).collect(Collectors.toList());
                    }
                })
                .map((KeyValueMapper<String, JsonObject, KeyValue<String, String>>) (key, value) -> {
                    if (ACTION_CREATE.equalsIgnoreCase(value.getString("action"))) {
                        return new KeyValue<>(value.getString("id"), value.getJsonObject("responder").encode());
                    } else {
                        return new KeyValue<>(value.getString("id"), null);
                    }
                })
                .toTable(Materialized.<String, String>as(keyValueStoreSupplier).withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

        return builder.build();
    }

    private double distanceUnit() {
        double max = baseDistance * (1 + distanceVariation);
        double min = baseDistance * (1 - distanceVariation);
        return ThreadLocalRandom.current().nextDouble(max - min) + min;
    }
}
