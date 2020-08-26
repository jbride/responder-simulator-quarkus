package com.redhat.emergency.response.responder.simulator;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import com.redhat.emergency.response.responder.simulator.model.ResponderLocation;

@ApplicationScoped
public class ResponderLocationRepository {

    private final Map<String, ResponderLocation> repository = new HashMap<>();

    public String put(ResponderLocation responderLocation) {
        repository.put(responderLocation.key(), responderLocation);
        return responderLocation.key();
    }

    public ResponderLocation get(String key) {
        return repository.get(key);
    }

    public void remove(String key) {
        repository.remove(key);
    }

}
