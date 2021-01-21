package com.redhat.emergency.response.responder.simulator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import com.redhat.emergency.response.responder.simulator.model.Coordinates;
import com.redhat.emergency.response.responder.simulator.model.MissionStep;
import com.redhat.emergency.response.responder.simulator.model.ResponderLocation;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

public class ResponderLocationRepositoryTest {

    @Test
    void testEncoding() {
        MissionStep step1 = new MissionStep(new Coordinates(new BigDecimal("30.12345"), new BigDecimal("-77.98765")), false, false);
        MissionStep step2 = new MissionStep(new Coordinates(new BigDecimal("40.12345"), new BigDecimal("-87.98765")), true, true);
        List<MissionStep> steps = Arrays.asList(step1, step2);
        Coordinates currentPosition = new Coordinates(new BigDecimal("29.12345"), new BigDecimal("-67.98765"));

        ResponderLocation responderLocation = new ResponderLocation("missionId", "responderId", "incidentId", steps, currentPosition, false, 1000.0);

        String encoded = Json.encode(responderLocation);

        JsonObject json = new JsonObject(encoded);
        assertThat(json, notNullValue());
        assertThat(json.getString("missionId"), equalTo("missionId"));
        assertThat(json.getString("responderId"), equalTo("responderId"));
        assertThat(json.getString("incidentId"), equalTo("incidentId"));
        assertThat(json.containsKey("currentPosition"), is(true));
        JsonObject currentPositionJson = json.getJsonObject("currentPosition");
        assertThat(currentPositionJson.getDouble("lat"), equalTo(29.12345));
        assertThat(currentPositionJson.getDouble("lon"), equalTo(-67.98765));
        assertThat(json.getBoolean("person"), is(false));
        assertThat(json.getString("status"), equalTo("CREATED"));
        assertThat(json.getBoolean("waiting"), is(false));
        assertThat(json.getDouble("distanceUnit"), equalTo(1000.0));
        assertThat(json.containsKey("queue"), is(true));
        JsonArray queue = json.getJsonArray("queue");
        assertThat(queue.size(), equalTo(2));
        JsonObject stepJson1 = queue.getJsonObject(0);
        assertThat(stepJson1.getBoolean("wayPoint"), equalTo(false));
        assertThat(stepJson1.getBoolean("destination"), equalTo(false));
        assertThat(stepJson1.getJsonObject("coordinates").getDouble("lat"), equalTo(30.12345));
        assertThat(stepJson1.getJsonObject("coordinates").getDouble("lon"), equalTo(-77.98765));
        JsonObject stepJson2 = queue.getJsonObject(1);
        assertThat(stepJson2.getBoolean("wayPoint"), equalTo(true));
        assertThat(stepJson2.getBoolean("destination"), equalTo(true));
        assertThat(stepJson2.getJsonObject("coordinates").getDouble("lat"), equalTo(40.12345));
        assertThat(stepJson2.getJsonObject("coordinates").getDouble("lon"), equalTo(-87.98765));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDecoding() throws Exception {

        String encoded = "{\"missionId\":\"missionId\",\"responderId\":\"responderId\",\"incidentId\":\"incidentId\"," +
                "\"queue\":[{\"coordinates\":{\"lat\":30.12345,\"lon\":-77.98765},\"wayPoint\":false,\"destination\":false}," +
                "{\"coordinates\":{\"lat\":40.12345,\"lon\":-87.98765},\"wayPoint\":true,\"destination\":true}]," +
                "\"currentPosition\":{\"lat\":29.12345,\"lon\":-67.98765},\"person\":true,\"waiting\":true," +
                "\"distanceUnit\":1000.0,\"status\":\"WAITING\"}";

        ResponderLocation responderLocation = Json.decodeValue(encoded, ResponderLocation.class);

        assertThat(responderLocation, notNullValue());
        assertThat(responderLocation.getMissionId(), equalTo("missionId"));
        assertThat(responderLocation.getResponderId(), equalTo("responderId"));
        assertThat(responderLocation.getIncidentId(), equalTo("incidentId"));
        assertThat(responderLocation.getStatus(), equalTo(ResponderLocation.Status.WAITING));
        assertThat(responderLocation.getCurrentPosition(), notNullValue());
        assertThat(responderLocation.getCurrentPosition().getLat(), equalTo(new BigDecimal("29.12345")));
        assertThat(responderLocation.getCurrentPosition().getLon(), equalTo(new BigDecimal("-67.98765")));
        assertThat(responderLocation.isPerson(), equalTo(true));
        assertThat((Double)field(responderLocation, "distanceUnit"), equalTo(1000.0));
        assertThat((Boolean)field(responderLocation, "waiting"), equalTo(true));
        Deque<MissionStep> queue = (Deque<MissionStep>) field(responderLocation, "queue");
        assertThat(queue.size(), equalTo(2));
        MissionStep step1 = queue.getFirst();
        assertThat(step1.getCoordinates().getLat(), equalTo(new BigDecimal("30.12345")));
        assertThat(step1.getCoordinates().getLon(), equalTo(new BigDecimal("-77.98765")));
        assertThat(step1.isWayPoint(), equalTo(false));
        assertThat(step1.isWayPoint(), equalTo(false));
        MissionStep step2 = queue.getLast();
        assertThat(step2.getCoordinates().getLat(), equalTo(new BigDecimal("40.12345")));
        assertThat(step2.getCoordinates().getLon(), equalTo(new BigDecimal("-87.98765")));
        assertThat(step2.isWayPoint(), equalTo(true));
        assertThat(step2.isWayPoint(), equalTo(true));
    }

    private Object field(Object obj, String field) throws Exception {
        Field privateField = obj.getClass().getDeclaredField(field);
        privateField.setAccessible(true);
        return privateField.get(obj);
    }

}
