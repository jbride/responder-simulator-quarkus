package com.redhat.emergency.response.responder.simulator.model;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.redhat.emergency.response.responder.simulator.DistanceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderLocation {

    private static final Logger log = LoggerFactory.getLogger(ResponderLocation.class);

    private String missionId;

    private String responderId;

    private String incidentId;

    @JsonProperty
    private Deque<MissionStep> queue;

    private Coordinates currentPosition;

    private boolean person;

    @JsonProperty
    private boolean waiting = false;

    @JsonProperty
    private double distanceUnit;

    public enum Status {CREATED, MOVING, WAITING, PICKEDUP, DROPPED};

    private Status status;

    private ResponderLocation() {
    }

    public ResponderLocation(String missionId, String responderId, String incidentId, List<MissionStep> missionSteps,
                             Coordinates currentPosition, boolean person, double distanceUnit) {
        this.missionId = missionId;
        this.responderId = responderId;
        this.incidentId = incidentId;
        this.queue = new LinkedList<>(missionSteps);
        this.currentPosition = currentPosition;
        this.person = person;
        this.distanceUnit = distanceUnit;
        this.status = Status.CREATED;
    }

    public Status getStatus() {
        return status;
    }

    public String getResponderId() {
        return responderId;
    }

    public String getMissionId() {
        return missionId;
    }

    public String getIncidentId() {
        return incidentId;
    }

    public Coordinates getCurrentPosition() {
        return currentPosition;
    }

    public boolean isPerson() {
        return person;
    }

    public String key() {
        return missionId;
    }

    public void calculateNextLocation() {
        if (isWaiting()) {
            log.debug("ResponderLocation " + missionId + " - Waiting on pickup");
            return;
        }
        if (queue.isEmpty()) {
            log.warn("MissionStep queue is empty.");
            return;
        }
        Coordinates current = currentPosition;
        log.debug("ResponderLocation " + missionId + " - Current location: " + currentPosition);
        MissionStep step = queue.peek();
        log.debug("ResponderLocation " + missionId + " - Next location: " + step.getCoordinates());
        Coordinates destination = step.getCoordinates();
        double distance = DistanceHelper.calculateDistance(currentPosition, step.getCoordinates());
        double intermediateDistance = 0.0;
        log.debug("ResponderLocation " + missionId + " - Distance to next location: " + distance + " meter");
        while (distance * 1.3 < distanceUnit) {
            step = queue.peek();
            if (step.isWayPoint() || step.isDestination()) {
                break;
            }
            step = queue.poll();
            current = step.getCoordinates();
            intermediateDistance = distance;
            log.debug("ResponderLocation " + missionId + " - Moving to next location: " + step.getCoordinates());
            step = queue.peek();
            destination = step.getCoordinates();
            double nextDistance = DistanceHelper.calculateDistance(current, destination);
            distance = distance + nextDistance;
            log.debug("ResponderLocation " + missionId + " - Distance to next location: " + distance + " meter");
        }
        if (distance > distanceUnit * 1.3) {
            log.debug("ResponderLocation " + missionId + " - Adding new intermediate step");
            Coordinates intermediateCoordinate = DistanceHelper.calculateIntermediateCoordinate(current, destination, distanceUnit - intermediateDistance);
            log.debug("ResponderLocation " + missionId + " - New step : " + intermediateCoordinate);
            MissionStep intermediateStep = new MissionStep(intermediateCoordinate, false, false);
            queue.addFirst(intermediateStep);
        }
    }

    public void moveToNextLocation() {
        MissionStep step = queue.poll();
        if (step == null) {
            log.warn("ResponderLocation " + missionId + " - MissionStep queue is empty");
            return ;
        }
        currentPosition = step.getCoordinates();
        if (person && step.isWayPoint()) {
            this.waiting = true;
            status = Status.WAITING;
        } else if (step.isWayPoint()) {
            status = Status.PICKEDUP;
        } else if (step.isDestination()) {
            status = Status.DROPPED;
        } else {
            status = Status.MOVING;
        }
    }

    public void continueMoving(Status status) {
        this.status = status;
        waiting = false;
    }

    private boolean isWaiting() {
        return (person && waiting);
    }

}
