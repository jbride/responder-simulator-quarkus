package com.redhat.emergency.response.responder.simulator;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.quarkus.vertx.web.Body;
import io.quarkus.vertx.web.Route;
import io.quarkus.vertx.web.RoutingExchange;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class RestApi {

    private static final Logger log = LoggerFactory.getLogger(RestApi.class);

    @Inject
    EventBus eventBus;

    @Route(path = "api/mission", methods = HttpMethod.POST)
    void missionStatus(@Body JsonObject mission, RoutingExchange ex) {
        if (!(mission.containsKey("missionId") && mission.containsKey("status"))) {
            ex.response().setStatusCode(500).end();
            return;
        }
        eventBus.request("simulator-responderlocation-status", mission).subscribe().with(m -> ex.response().setStatusCode(200).end());
    }

    @Route(path = "api/clear", methods = HttpMethod.POST)
    void clear(RoutingExchange ex) {
        eventBus.request("simulator-clear", new JsonObject()).subscribe().with(m -> ex.response().setStatusCode(200).end());
    }

}
