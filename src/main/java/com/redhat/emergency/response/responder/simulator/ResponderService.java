package com.redhat.emergency.response.responder.simulator;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class ResponderService {

    private static final Logger log = LoggerFactory.getLogger(ResponderService.class);

    @ConfigProperty(name = "responder-service.url")
    String serviceUrl;

    @ConfigProperty(name = "responder-service.request-uri")
    String requestUri;

    @Inject
    Vertx vertx;

    private WebClient client;

    void onStart(@Observes StartupEvent e) {
        int servicePort = serviceUrl.contains(":") ? Integer.parseInt(serviceUrl.substring(serviceUrl.indexOf(":") + 1)) : 8080;
        String serviceHost = serviceUrl.contains(":") ? serviceUrl.substring(0, serviceUrl.indexOf(":")) : serviceUrl;
        client = WebClient.create(vertx, new WebClientOptions().setDefaultHost(serviceHost).setDefaultPort(servicePort).setMaxPoolSize(100).setHttp2MaxPoolSize(100));
    }

    public Uni<Boolean> isPerson(String responderId) {
        return client.get(requestUri + responderId).send().onItem().transform(resp -> {
            if (resp.statusCode() != 200) {
                log.error("Error when calling responder service. Return code = " + resp.statusCode());
                return false;
            }
            JsonObject json = resp.bodyAsJsonObject();
            if (!json.containsKey("person")) {
                log.error("Error when calling responder service. Response does not contain property 'person'. Response: " + json.encode());
                return false;
            }
            log.debug("Responder " + responderId + ": person = " + json.getBoolean("person"));
            return json.getBoolean("person");
        });
    }

}
