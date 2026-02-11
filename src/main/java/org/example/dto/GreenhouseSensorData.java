package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GreenhouseSensorData implements Serializable {
    @JsonProperty("sensor_id")
    private String sensorId;
    
    @JsonProperty("greenhouse_id")
    private String greenhouseId;
    
    private long timestamp;
    
    private Location location;
    
    private Metrics metrics;
    
    private Device device;
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Location implements Serializable {
        private double x;
        private double y;
        private double z;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Metrics implements Serializable {
        private double temperature;
        private double humidity;
        private int co2;
        private int light;
        
        @JsonProperty("soil_temperature")
        private double soilTemperature;
        
        @JsonProperty("soil_moisture")
        private double soilMoisture;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Device implements Serializable {
        private int battery;
        private String status;
        private String firmware;
    }
}