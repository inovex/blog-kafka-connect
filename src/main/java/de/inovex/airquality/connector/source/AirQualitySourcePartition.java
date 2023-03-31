package de.inovex.airquality.connector.source;

import de.inovex.airquality.connector.source.config.BoundingBox;
import de.inovex.airquality.data.DataService;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/***
 * an AirQualitySourcePartition represents one bounding box location.
 * This class holds the necessary state for retrieving the data for the location.
 */
class AirQualitySourcePartition {

    private BoundingBox location;
    private DataService dataService;
    private LocalDateTime lastTimeStamp;
    private Long lastID;

    /**
     * creates a new partition instance using the given location and data service
     * @param location the location of this partition
     * @param dataService the data service of this partition
     */
    AirQualitySourcePartition(BoundingBox location, DataService dataService) {
        this.location = location;
        this.dataService = dataService;
    }


    /**
     * returns the location of this partition
     * @return the location of this partition
     */
    public Object getLocation() { return location; }


    /**
     * returns the timestamp of the last processed record
     * @return the timestamp of the last processed record
     */
    public LocalDateTime getLastTimeStamp() { return lastTimeStamp; }

    /**
     * returns the ID of the last processed record
     * @return the ID of the last processed record
     */
    public Long getLastID() { return lastID; }

    /**
     * returns the data service which returns records for this partition's location
     * @return the ID of the last processed record
     */
    public DataService getDataService() { return dataService; }

    /**
     * sets the timestamp of the last processed record
     * @param lastTimeStamp the timestamp of the last processed record
     */
    public void setLastTimeStamp(LocalDateTime lastTimeStamp) {
        this.lastTimeStamp = lastTimeStamp;
    }

    /**
     * sets the ID of the last processed record
     * @param lastID the ID of the last processed record
     */
    public void setLastID(Long lastID) {
        this.lastID = lastID;
    }

    /**
     * returns the source partition config map
     * @return the source partition config map
     */
    public Map<String, String> getSourcePartition() {
        return Collections.singletonMap("LOCATION", location.toString());
    }

    /**
     * returns the current offset config map of this source partition
     * @return the current offset config map of this source partition
     */
    public Map<String, String> getSourceOffset() {
        final Map<String, String> sourceOffset = new HashMap<>();
        // TODO: change keys
        sourceOffset.put("LAST_ID", String.valueOf(lastID));
        sourceOffset.put("LAST_TIMESTAMP", lastTimeStamp.toString());
        return sourceOffset;
    }

    /**
     * sets the offset of this source partition according to the given source offset config map
     * @param offset the source offset config map
     */
    public void setSourceOffset(Map<String, Object> offset) {
        if (offset != null) {
            lastID = Long.parseLong((String) offset.get("LAST_ID"));
            lastTimeStamp = LocalDateTime.parse((String) offset.get("LAST_TIMESTAMP"));
        }
    }

}
