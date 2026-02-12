package org.example.agriculture.dto.ods;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FarmOperationData implements Serializable {
    @JsonProperty("operation_id")
    private String operationId;
    
    @JsonProperty("data_type")
    private String dataType;
    
    @JsonProperty("greenhouse_id")
    private String greenhouseId;
    
    private long timestamp;
    
    private Operator operator;
    
    private Operation operation;
    
    @JsonProperty("control_params")
    private ControlParams controlParams;
    
    private Decision decision;
    
    private Result result;
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Operator implements Serializable {
        private String id;
        private String name;
        private String role;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Operation implements Serializable {
        @JsonProperty("operation_type")
        private String operationType;
        
        @JsonProperty("target_zone")
        private String targetZone;
        
        @JsonProperty("duration_sec")
        private int durationSec;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ControlParams implements Serializable {
        @JsonProperty("irrigation_mode")
        private String irrigationMode;
        
        @JsonProperty("water_volume_l")
        private Double waterVolumeL;
        
        @JsonProperty("fertilizer_type")
        private String fertilizerType;
        
        @JsonProperty("fertilizer_amount_kg")
        private Double fertilizerAmountKg;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Decision implements Serializable {
        @JsonProperty("trigger_source")
        private String triggerSource;
        
        private String reason;
        
        @JsonProperty("expected_effect")
        private String expectedEffect;
        
        @JsonProperty("related_sensor_snapshot")
        private RelatedSensorSnapshot relatedSensorSnapshot;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class RelatedSensorSnapshot implements Serializable {
        @JsonProperty("soil_moisture")
        private Double soilMoisture;
        
        @JsonProperty("soil_ec")
        private Double soilEc;
        
        @JsonProperty("soil_temperature")
        private Double soilTemperature;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Result implements Serializable {
        @JsonProperty("execution_status")
        private String executionStatus;
        
        @JsonProperty("audit_required")
        private boolean auditRequired;
        
        private String remark;
    }
}