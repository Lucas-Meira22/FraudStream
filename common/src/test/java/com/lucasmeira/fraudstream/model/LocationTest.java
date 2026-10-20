package com.lucasmeira.fraudstream.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

class LocationTest {

    @Test
    void createLocationWithValidCoordinates() {
        Location lisbon = new Location(38.72, -9.14);

        assertThat(lisbon.latitude()).isEqualTo(38.72);
        assertThat(lisbon.longitude()).isEqualTo(-9.14);
    }

    @ParameterizedTest
    @ValueSource(doubles = {-90.01, 90.01, 1000})
    void rejectsLatitudeOutOfRange(double badLatitude) {
        assertThatThrownBy(() -> new Location(badLatitude, 0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(doubles = {-180.01, 180.01, 1000})
    void rejectsLongitudeOutOfRange(double badLongitude) {
        assertThatThrownBy(() -> new Location(0, badLongitude))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"90, 180", "-90, -180", "0, 0"})
    void acceptsBoundaryCoordinates(double lat, double lon) {
        assertThatNoException().isThrownBy(() -> new Location(lat, lon));
    }

    @Test
    void rejectsNaN() {
        assertThatThrownBy(() -> new Location(Double.NaN, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Location(0, Double.NaN))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void distanceToSamePointIsZero() {
        Location lisbon = new Location(38.7223, -9.1393);

        assertThat(lisbon.distanceKmTo(lisbon)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    void distanceBetweenLisbonAndMadrid() {
        Location lisbon = new Location(38.7223, -9.1393);
        Location madrid = new Location(40.4168, -3.7038);

        assertThat(lisbon.distanceKmTo(madrid)).isCloseTo(503.0, within(5.0));
    }

    @Test
    void distanceIsSymmetric() {
        Location lisbon = new Location(38.7223, -9.1393);
        Location madrid = new Location(40.4168, -3.7038);

        assertThat(lisbon.distanceKmTo(madrid)).isCloseTo(madrid.distanceKmTo(lisbon), within(1e-9));
    }

    @Test
    void distanceAcrossAntimeridianTakesShortWay() {
        Location west = new Location(0, 179);
        Location east = new Location(0, -179);

        // 2 degrees of longitude at the equator, not 358
        assertThat(west.distanceKmTo(east)).isCloseTo(222.4, within(1.0));
    }
}
