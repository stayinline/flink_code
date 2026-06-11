package org.example.job.cdc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.CdcChangeEvent;

import java.util.Optional;

/**
 * 解析 {@link com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema} 输出的 JSON。
 */
public final class CdcDebeziumJsonParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CdcDebeziumJsonParser() {
    }

    public static Optional<CdcChangeEvent> parse(String json) {
        if (json == null || json.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode root = MAPPER.readTree(json);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;

            String op = text(payload, "op");
            JsonNode source = payload.get("source");
            JsonNode dataNode = selectDataNode(payload, op);

            if (dataNode == null || dataNode.isNull()) {
                return Optional.empty();
            }

            CdcChangeEvent event = new CdcChangeEvent();
            event.setOp(op);
            event.setRawJson(json);

            if (source != null) {
                event.setDatabase(text(source, "db"));
                event.setTable(text(source, "table"));
                event.setBinlogFile(text(source, "file"));
                event.setBinlogPos(longVal(source, "pos"));
                String snapshot = text(source, "snapshot");
                event.setSnapshotLabel(snapshot);
                event.setSnapshotPhase(isSnapshotPhase(snapshot, op));
            } else {
                event.setSnapshotPhase("r".equalsIgnoreCase(op));
            }

            event.setPrimaryKeyId(longVal(dataNode, "id"));
            event.setStudentId(text(dataNode, "student_id"));
            event.setCourseId(text(dataNode, "course_id"));
            event.setEnrollStatus(text(dataNode, "enroll_status"));
            event.setEventTsMs(longVal(payload, "ts_ms") != null ? longVal(payload, "ts_ms") : 0L);
            event.setDbUpdatedAtMs(parseUpdatedAtMs(dataNode));

            return Optional.of(event);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static JsonNode selectDataNode(JsonNode payload, String op) {
        if ("d".equalsIgnoreCase(op)) {
            return payload.has("before") ? payload.get("before") : payload.get("after");
        }
        return payload.get("after");
    }

    private static boolean isSnapshotPhase(String snapshot, String op) {
        if (snapshot != null) {
            return "true".equalsIgnoreCase(snapshot)
                    || "first".equalsIgnoreCase(snapshot)
                    || "last_in_data_collection".equalsIgnoreCase(snapshot)
                    || snapshot.contains("incremental");
        }
        return "r".equalsIgnoreCase(op);
    }

    private static String text(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).asText();
    }

    private static Long longVal(JsonNode node, String field) {
        if (node == null || !node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).asLong();
    }

    /** updated_at 可能是毫秒数或 ISO 字符串（Demo 以毫秒为主） */
    private static Long parseUpdatedAtMs(JsonNode dataNode) {
        if (dataNode == null || !dataNode.has("updated_at") || dataNode.get("updated_at").isNull()) {
            return null;
        }
        JsonNode n = dataNode.get("updated_at");
        if (n.isNumber()) {
            long v = n.asLong();
            return v < 1_000_000_000_000L ? v * 1000 : v;
        }
        return null;
    }
}
