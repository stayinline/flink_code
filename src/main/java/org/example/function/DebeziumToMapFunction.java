package org.example.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DebeziumToMapFunction implements MapFunction<String, Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumToMapFunction.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> map(String value) throws Exception {
        int retryCount = 0;
        final int maxRetries = 3;
        
        while (retryCount < maxRetries) {
            try {
                JsonNode rootNode = objectMapper.readTree(value);
                JsonNode payloadNode = rootNode.get("payload");
                
                if (payloadNode == null) {
                    LOG.warn("Invalid CDC event format: missing payload field");
                    return null;
                }
                
                JsonNode afterNode = payloadNode.get("after");
                if (afterNode != null && !afterNode.isNull()) {
                    Map<String, Object> result = jsonNodeToMap(afterNode);
                    if (result == null || result.isEmpty()) {
                        LOG.debug("CDC event with empty 'after' field");
                        return null;
                    }
                    return result;
                }
                
                LOG.debug("CDC event with null 'after' field (possibly a delete event)");
                return null;
            } catch (Exception e) {
                retryCount++;
                if (retryCount < maxRetries) {
                    LOG.warn("Failed to parse CDC event, retrying {}/{}: {}", retryCount, maxRetries, e.getMessage());
                    Thread.sleep(500 * retryCount);
                } else {
                    LOG.error("Failed to parse CDC event after {} retries: {}", maxRetries, value, e);
                    return null;
                }
            }
        }
        return null;
    }

    private Map<String, Object> jsonNodeToMap(JsonNode node) {
        Map<String, Object> map = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();
            JsonNode valueNode = field.getValue();
            
            if (valueNode.isTextual()) {
                map.put(key, valueNode.asText());
            } else if (valueNode.isInt()) {
                map.put(key, valueNode.asInt());
            } else if (valueNode.isLong()) {
                map.put(key, valueNode.asLong());
            } else if (valueNode.isDouble()) {
                map.put(key, valueNode.asDouble());
            } else if (valueNode.isBoolean()) {
                map.put(key, valueNode.asBoolean());
            } else if (valueNode.isNull()) {
                map.put(key, null);
            } else if (valueNode.isObject()) {
                map.put(key, jsonNodeToMap(valueNode));
            }
        }
        
        return map;
    }
}