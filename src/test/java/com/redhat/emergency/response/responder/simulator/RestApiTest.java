package com.redhat.emergency.response.responder.simulator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

import com.redhat.emergency.response.responder.simulator.streams.KafkaResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class RestApiTest {

    @InjectMock
    Simulator simulator;

    @Captor
    ArgumentCaptor<Message<JsonObject>> messageCaptor;

    @BeforeEach
    void init() {
        openMocks(this);
    }

    @Test
    void testMissionStatus() {

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject());
            return null;
        }).when(simulator).processResponderLocationStatus(any());

        String body = "{\"missionId\":\"qwerty\", \"status\":\"PICKEDUP\"}";

        RestAssured.given().body(body).post("/api/mission").then()
                .assertThat()
                .statusCode(200);
        verify(simulator).processResponderLocationStatus(messageCaptor.capture());
        Message<JsonObject> captured = messageCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.body(), notNullValue());
        JsonObject json = captured.body();
        assertThat(json.getString("missionId"), equalTo("qwerty"));
        assertThat(json.getString("status"), equalTo("PICKEDUP"));
    }

    @Test
    void testMissionStatusMissingFields() {

        String body = "{\"missionId\":\"qwerty\"}";

        RestAssured.given().body(body).post("/api/mission").then()
                .assertThat()
                .statusCode(500);
        verify(simulator, never()).processResponderLocationStatus(any());
    }
}
