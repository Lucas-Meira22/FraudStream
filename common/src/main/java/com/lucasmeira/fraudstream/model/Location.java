package com.lucasmeira.fraudstream.model;

public record Location(double latitude, double longitude) {

    private static final double EARTH_RADIUS_KM = 6371.0;

    public Location {
        requireInRange("latitude", latitude, -90, 90);
        requireInRange("longitude", longitude, -180, 180);
    }

    // Haversine: great-circle distance, the way a plane flies
    public double distanceKmTo(Location other) {
        double lat1 = Math.toRadians(latitude);
        double lat2 = Math.toRadians(other.latitude);
        double deltaLat = Math.toRadians(other.latitude - latitude);
        double deltaLon = Math.toRadians(other.longitude - longitude);

        double a = Math.pow(Math.sin(deltaLat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(deltaLon / 2), 2);

        return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(a));
    }

    private static void requireInRange(String name, double value, double min, double max) {
        if (Double.isNaN(value) || value < min || value > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max + ", got " + value);
        }
    }
}
