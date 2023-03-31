package de.inovex.airquality.connector.source.config;

import java.util.Arrays;
import java.util.Objects;

public class BoundingBox {

    private final float lat1;

    private final float lon1;

    private final float lat2;

    private final float lon2;

    public BoundingBox(float lat1, float lon1, float lat2, float lon2) {
        this.lat1 = lat1;
        this.lon1 = lon1;
        this.lat2 = lat2;
        this.lon2 = lon2;
    }

    public static BoundingBox fromString(String boundingBox) {
        Float[] parts = Arrays.stream(boundingBox.split(";"))
                .map(Float::parseFloat)
                .toArray(Float[]::new);
        return new BoundingBox(parts[0], parts[1], parts[2], parts[3]);
    }

    public float getLat1() {
        return lat1;
    }

    public float getLon1() {
        return lon1;
    }

    public float getLat2() {
        return lat2;
    }

    public float getLon2() {
        return lon2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoundingBox that = (BoundingBox) o;
        return Float.compare(that.lat1, lat1) == 0
                && Float.compare(that.lon1, lon1) == 0
                && Float.compare(that.lat2, lat2) == 0
                && Float.compare(that.lon2, lon2) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lat1, lon1, lat2, lon2);
    }

    @Override
    public String toString() {
        return lat1+";"+lon1+";"+lat2+";"+lon2;
    }
}