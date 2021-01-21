package com.redhat.emergency.response.responder.simulator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.openMocks;

import javax.inject.Inject;

import com.redhat.emergency.response.responder.simulator.streams.KafkaResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class MissionEventSourceTest {

    @InjectMock
    Simulator simulator;

    @Inject
    MissionEventSource missionEventSource;

    @Captor
    ArgumentCaptor<Message<JsonObject>> messageCaptor;

    @BeforeEach
    void init() {
        openMocks(this);
    }

    @Test
    void testProcessMessage() {
        String payload = "{" +
                "    \"id\": \"ef2c797d-a8b7-4b47-a50e-b38b682e92c7\"," +
                "    \"invokingService\": \"MissionService\"," +
                "    \"timestamp\": 1597697392063," +
                "    \"messageType\": \"MissionStartedEvent\"," +
                "    \"body\": {" +
                "        \"id\": \"979a5f16-7421-41dd-bad5-26f405d693fe\"," +
                "        \"incidentId\": \"incident236\"," +
                "        \"responderId\": \"responder123\"," +
                "        \"responderStartLat\": 34.18323," +
                "        \"responderStartLong\": -77.90999," +
                "        \"incidentLat\": 34.18408," +
                "        \"incidentLong\": -77.84856," +
                "        \"destinationLat\": 34.1706," +
                "        \"destinationLong\": -77.949," +
                "        \"responderLocationHistory\": []," +
                "        \"status\": \"CREATED\"," +
                "        \"steps\": [" +
                "            {" +
                "                \"lat\": 34.1827," +
                "                \"lon\": -77.9106," +
                "                \"wayPoint\": false," +
                "                \"destination\": false" +
                "            }," +
                "            {" +
                "                \"lat\": 34.1842," +
                "                \"lon\": -77.9125," +
                "                \"wayPoint\": false," +
                "                \"destination\": false" +
                "            }" +
                "        ]" +
                "    }" +
                "}";

        doAnswer(invocation -> {
            Message<JsonObject> msg = invocation.getArgument(0);
            msg.replyAndForget(new JsonObject());
            return null;
        }).when(simulator).processMissionCreated(any());

        MessageWithAck<String> message = MessageWithAck.of(payload);
        missionEventSource.process(message).await().indefinitely();
        verify(simulator).processMissionCreated(messageCaptor.capture());
        Message<JsonObject> captured = messageCaptor.getValue();
        assertThat(captured, notNullValue());
        assertThat(captured.body(), notNullValue());
        JsonObject body = captured.body();
        assertThat(body.getString("id"), equalTo("979a5f16-7421-41dd-bad5-26f405d693fe"));
        assertThat(body.getJsonArray("steps"), notNullValue());
        assertThat(body.getJsonArray("steps").size(), equalTo(2));
        assertThat(message.acked(), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testProcessMessageWhenNotMissionStartedEvent() {
        String payload = "{" +
                "    \"id\": \"ef2c797d-a8b7-4b47-a50e-b38b682e92c7\"," +
                "    \"invokingService\": \"MissionService\"," +
                "    \"timestamp\": 1597697392063," +
                "    \"messageType\": \"MissionCompletedEvent\"," +
                "    \"body\": {" +
                "        \"id\": \"979a5f16-7421-41dd-bad5-26f405d693fe\"," +
                "        \"incidentId\": \"incident236\"," +
                "        \"responderId\": \"responder123\"," +
                "        \"responderStartLat\": 34.18323," +
                "        \"responderStartLong\": -77.90999," +
                "        \"incidentLat\": 34.18408," +
                "        \"incidentLong\": -77.84856," +
                "        \"destinationLat\": 34.1706," +
                "        \"destinationLong\": -77.949," +
                "        \"responderLocationHistory\": []," +
                "        \"status\": \"CREATED\"," +
                "        \"steps\": [" +
                "            {" +
                "                \"lat\": 34.1827," +
                "                \"lon\": -77.9106," +
                "                \"wayPoint\": false," +
                "                \"destination\": false" +
                "            }," +
                "            {" +
                "                \"lat\": 34.1842," +
                "                \"lon\": -77.9125," +
                "                \"wayPoint\": false," +
                "                \"destination\": false" +
                "            }" +
                "        ]" +
                "    }" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload);
        missionEventSource.process(message).await().indefinitely();
        verify(simulator, never()).processMissionCreated(any(io.vertx.mutiny.core.eventbus.Message.class));
        assertThat(message.acked(), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testProcessMessageWhenUnexpectedMessage() {
        String payload = "test";

        MessageWithAck<String> message = MessageWithAck.of(payload);
        missionEventSource.process(message).await().indefinitely();
        verify(simulator, never()).processMissionCreated(any(io.vertx.mutiny.core.eventbus.Message.class));
        assertThat(message.acked(), is(true));
    }

}
