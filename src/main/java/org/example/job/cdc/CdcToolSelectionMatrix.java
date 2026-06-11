package org.example.job.cdc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Canal / Maxwell / Debezium / Flink CDC 选型矩阵（文档与单测共用）。
 */
public final class CdcToolSelectionMatrix {

    private CdcToolSelectionMatrix() {
    }

    public static Map<String, Map<String, String>> build() {
        Map<String, Map<String, String>> matrix = new LinkedHashMap<>();

        matrix.put("Canal", dims(
                "无内置全量（需 mysqldump 配合）",
                "Canal Server 集群，HA 成熟",
                "阿里系，直连 Kafka/RocketMQ，中文社区大",
                "at-least-once 为主，幂等靠下游"
        ));

        matrix.put("Maxwell", dims(
                "无全量（bootstrap 可选）",
                "单进程为主，轻量",
                "Kafka 友好，JSON 简洁",
                "at-least-once"
        ));

        matrix.put("Debezium", dims(
                "快照模式可选（initial/blocking）",
                "Kafka Connect 分布式",
                "Connect 生态，多数据库",
                "exactly-once（Kafka Connect + EOS）"
        ));

        matrix.put("Flink CDC", dims(
                "增量快照 FLIP-27，全+增一体无锁",
                "Flink 集群天然分布式",
                "与 Flink SQL/CK 深度集成",
                "Flink Checkpoint exactly-once"
        ));

        return matrix;
    }

    private static Map<String, String> dims(String full, String distributed, String ecosystem, String semantic) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("全量能力", full);
        m.put("分布式", distributed);
        m.put("生态", ecosystem);
        m.put("语义", semantic);
        return m;
    }
}
