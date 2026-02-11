//package org.example.job;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.example.dto.KafkaData;
//
//public class Test {
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//
//    public static void main(String[] args) throws JsonProcessingException {
//        String s = "{\"id\":1001,\"message\":\"系统监控数据\",\"value\":92.5,\"event_time\":\"2025-09-29 16:51:28\"}";
//        KafkaData kafkaData = objectMapper.readValue(s, KafkaData.class);
//
//    }
//
//
//}
