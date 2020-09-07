package com.redhat.emergency.response.responder.simulator.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Coordinates {

    private BigDecimal lat;

    private BigDecimal lon;

    private Coordinates() {}

    public Coordinates(BigDecimal lat, BigDecimal lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public BigDecimal getLat() {
        return lat;
    }

    @JsonIgnore
    public double getLatD() {
        return lat.doubleValue();
    }

    public BigDecimal getLon() {
        return lon;
    }

    @JsonIgnore
    public double getLonD() {
        return lon.doubleValue();
    }

    @Override
    public String toString() {
        return "[" + lat.toString() + "," + lon.toString() + "]";
    }
}
