package com.redhat.emergency.response.responder.simulator.model;

import java.math.BigDecimal;

public class Coordinates {

    private final BigDecimal lat;

    private final BigDecimal lon;

    public Coordinates(BigDecimal lat, BigDecimal lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public BigDecimal getLat() {
        return lat;
    }

    public double getLatD() {
        return lat.doubleValue();
    }

    public BigDecimal getLon() {
        return lon;
    }

    public double getLonD() {
        return lon.doubleValue();
    }

    @Override
    public String toString() {
        return "[" + lat.toString() + "," + lon.toString() + "]";
    }
}
