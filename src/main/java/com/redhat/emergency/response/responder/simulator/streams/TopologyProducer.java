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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ProcessorContext;
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
                .transform(ResponderEventTransformer::new)
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
                        JsonArray responders = value.getJsonArray("responders");
                        return responders.stream().map(o -> (JsonObject)o).map(resp -> new JsonObject().put("action", ACTION_CREATE)
                                .put("id", resp.getString("id"))
                                .put("responder", resp.put("distanceUnit", distanceUnit())))
                                .collect(Collectors.toList());
                    } else {
                        JsonArray responderIds = value.getJsonArray("responders");
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

    public static class ResponderEventTransformer implements Transformer<String, String, KeyValue<String, JsonObject>> {
        private ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, JsonObject> transform(String key, String value) {
            // check cloudevent specversion header
            Header specVersion = context().headers().lastHeader("ce_specversion");
            if (specVersion == null || !("1.0".equals(new String(specVersion.value())))) {
                log.warn("Message is not a CloudEvent");
                return new KeyValue<>(key, new JsonObject());
            }
            // check cloudevent datacontenttype header
            Header dataContentType = context().headers().lastHeader("ce_datacontenttype");
            if (dataContentType == null || !("application/json".equals(new String(dataContentType.value())))) {
                log.warn("CloudEvent data content type header not set or not equal to application/json");
                return new KeyValue<>(key, new JsonObject());
            }
            // check cloudevent type header
            Header messageType = context().headers().lastHeader("ce_type");
            if (messageType == null || messageType.value() == null || messageType.value().length == 0 ) {
                log.warn("CloudEvent messageType not set");
                return new KeyValue<>(key, new JsonObject());
            }
            try {
                JsonObject jsonObject = new JsonObject(value);
                log.debug("Processing message: " + value);
                jsonObject.put("messageType", new String(messageType.value()));
                return new KeyValue<>(key, jsonObject);
            } catch (Exception e) {
                log.warn("Unexpected message which is not a valid JSON object");
                return new KeyValue<>(key, new JsonObject());
            }
        }

        @Override
        public void close() {
        }

        public ProcessorContext context() {
            return context;
        }
    }
}
