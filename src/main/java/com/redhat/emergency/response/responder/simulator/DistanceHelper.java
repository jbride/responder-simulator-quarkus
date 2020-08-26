package com.redhat.emergency.response.responder.simulator;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.redhat.emergency.response.responder.simulator.model.Coordinates;

/**
 *  Reference: http://www.movable-type.co.uk/scripts/latlong.html
 */
public class DistanceHelper {

    static final int R = 6371; // Radius of the earth

    public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c * 1000;
    }

    public static double calculateDistance(Coordinates c1, Coordinates c2) {
        return calculateDistance(c1.getLatD(), c1.getLonD(), c2.getLatD(), c2.getLonD());
    }

    public static Coordinates calculateIntermediateCoordinate(Coordinates c1, Coordinates c2, double distance) {

        double latR1 = Math.toRadians(c1.getLatD());
        double latR2 = Math.toRadians(c2.getLatD());
        double lonR1 = Math.toRadians(c1.getLonD());
        double longDiff= Math.toRadians(c2.getLonD()-c1.getLonD());
        double y= Math.sin(longDiff)*Math.cos(latR2);
        double x=Math.cos(latR1)*Math.sin(latR2)-Math.sin(latR1)*Math.cos(latR2)*Math.cos(longDiff);

        double bearing = (Math.toDegrees(Math.atan2(y, x))+360)%360;

        double bearingR = Math.toRadians(bearing);

        double distFrac = distance / (R * 1000);

        double a = Math.sin(distFrac) * Math.cos(latR1);
        double latDest = Math.asin(Math.sin(latR1) * Math.cos(distFrac) + a * Math.cos(bearingR));
        double lonDest = lonR1 + Math.atan2(Math.sin(bearingR) * a, Math.cos(distFrac) - Math.sin(latR1) * Math.sin(latDest));

        return new Coordinates(toBigDecimal(Math.toDegrees(latDest),4), toBigDecimal(Math.toDegrees(lonDest), 4));

    }

    private static BigDecimal toBigDecimal(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd;
    }

}
