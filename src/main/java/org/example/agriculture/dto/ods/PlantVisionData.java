package org.example.agriculture.dto.ods;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlantVisionData implements Serializable {
    @JsonProperty("plant_detect_id")
    private String plantDetectId;
    
    @JsonProperty("greenhouse_id")
    private String greenhouseId;
    
    @JsonProperty("zone_id")
    private String zoneId;
    
    @JsonProperty("camera_id")
    private String cameraId;
    
    private long timestamp;
    
    @JsonProperty("plant_basic")
    private PlantBasic plantBasic;
    
    @JsonProperty("plant_health")
    private PlantHealth plantHealth;
    
    @JsonProperty("fruit_info")
    private FruitInfo fruitInfo;
    
    @JsonProperty("stress_analysis")
    private StressAnalysis stressAnalysis;
    
    private double confidence;
    
    @JsonProperty("model_version")
    private String modelVersion;
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PlantBasic implements Serializable {
        @JsonProperty("crop_type")
        private String cropType;
        
        @JsonProperty("growth_stage")
        private String growthStage;
        
        @JsonProperty("plant_height_cm")
        private double plantHeightCm;
        
        @JsonProperty("leaf_count")
        private int leafCount;
        
        @JsonProperty("canopy_coverage")
        private double canopyCoverage;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PlantHealth implements Serializable {
        @JsonProperty("leaf_color_index")
        private double leafColorIndex;
        
        @JsonProperty("chlorophyll_index")
        private double chlorophyllIndex;
        
        @JsonProperty("wilting_score")
        private double wiltingScore;
        
        @JsonProperty("disease_risk")
        private String diseaseRisk;
        
        @JsonProperty("pest_risk")
        private String pestRisk;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FruitInfo implements Serializable {
        @JsonProperty("fruit_count")
        private int fruitCount;
        
        @JsonProperty("avg_fruit_diameter_mm")
        private double avgFruitDiameterMm;
        
        @JsonProperty("fruit_color_stage")
        private String fruitColorStage;
    }
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StressAnalysis implements Serializable {
        @JsonProperty("water_stress")
        private String waterStress;
        
        @JsonProperty("nutrient_stress")
        private String nutrientStress;
        
        @JsonProperty("light_stress")
        private String lightStress;
    }
}