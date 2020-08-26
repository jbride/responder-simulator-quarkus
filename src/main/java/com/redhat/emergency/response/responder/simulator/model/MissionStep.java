package com.redhat.emergency.response.responder.simulator.model;

public class MissionStep {

    private final Coordinates coordinates;

    private final boolean wayPoint;

    private final boolean destination;

    public MissionStep(Coordinates coordinates, boolean wayPoint, boolean destination) {
        this.coordinates = coordinates;
        this.wayPoint = wayPoint;
        this.destination = destination;
    }

    public Coordinates getCoordinates() {
        return coordinates;
    }

    public boolean isWayPoint() {
        return wayPoint;
    }

    public boolean isDestination() {
        return destination;
    }
}
