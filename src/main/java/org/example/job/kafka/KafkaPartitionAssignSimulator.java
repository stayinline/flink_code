package org.example.job.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 模拟 Kafka 分区与 Flink Source Subtask 分配（RangeAssignor 简化版）。
 */
public final class KafkaPartitionAssignSimulator {

    private KafkaPartitionAssignSimulator() {
    }

    /**
     * @return subtaskIndex → 分配到的 partition 列表
     */
    public static Map<Integer, List<Integer>> assignPartitions(int partitionCount, int sourceParallelism) {
        Map<Integer, List<Integer>> assignment = new HashMap<>();
        for (int p = 0; p < sourceParallelism; p++) {
            assignment.put(p, new ArrayList<>());
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            int subtask = partition % sourceParallelism;
            assignment.get(subtask).add(partition);
        }
        return assignment;
    }

    public static int idleSubtaskCount(int partitionCount, int sourceParallelism) {
        Map<Integer, List<Integer>> assignment = assignPartitions(partitionCount, sourceParallelism);
        int idle = 0;
        for (List<Integer> parts : assignment.values()) {
            if (parts.isEmpty()) {
                idle++;
            }
        }
        return idle;
    }

    public static int subtaskForPartition(int partition, int sourceParallelism) {
        return partition % sourceParallelism;
    }
}
