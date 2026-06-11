package org.example.job.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 简化 Flink Runtime 模型：JM 调度 TM 上 Slot 中的 Subtask（Task 线程）。
 */
public final class RuntimeModelSimulator {

    private RuntimeModelSimulator() {
    }

    public static RuntimeLayout layout(int taskManagers, int slotsPerTm, int jobParallelism) {
        int totalSlots = taskManagers * slotsPerTm;
        boolean slotSufficient = totalSlots >= jobParallelism;
        return new RuntimeLayout(taskManagers, slotsPerTm, totalSlots, jobParallelism, slotSufficient);
    }

    /** 模拟反压从 Sink 向上游传播 */
    public static BackpressurePropagation simulateBackpressure(int operatorCount, int sinkSleepMs) {
        List<OperatorPressure> chain = new ArrayList<>();
        for (int i = 0; i < operatorCount; i++) {
            chain.add(new OperatorPressure("op-" + i, 0, 0, 1000));
        }
        chain.get(operatorCount - 1).busyMs = Math.min(1000, sinkSleepMs * 2);
        chain.get(operatorCount - 1).backpressuredMs = 0;

        for (int i = operatorCount - 2; i >= 0; i--) {
            OperatorPressure downstream = chain.get(i + 1);
            OperatorPressure current = chain.get(i);
            if (downstream.busyMs > 800) {
                current.backpressuredMs = 600;
                current.busyMs = 300;
            } else {
                current.busyMs = 800;
                current.backpressuredMs = 0;
            }
        }
        return new BackpressurePropagation(chain);
    }

    /** 热点 key：同一 keyGroup 始终落同一 subtask */
    public static int subtaskForKey(String key, int parallelism, int maxParallelism) {
        int keyGroup = Math.floorMod(key.hashCode(), maxParallelism);
        return keyGroup * parallelism / maxParallelism;
    }

    public static Map<Integer, Long> routeByKey(List<String> keys, int parallelism, int maxParallelism) {
        Map<Integer, Long> load = new HashMap<>();
        for (String key : keys) {
            int subtask = subtaskForKey(key, parallelism, maxParallelism);
            load.merge(subtask, 1L, Long::sum);
        }
        return load;
    }

    public static class RuntimeLayout {
        public final int taskManagers;
        public final int slotsPerTm;
        public final int totalSlots;
        public final int jobParallelism;
        public final boolean slotSufficient;

        public RuntimeLayout(int taskManagers, int slotsPerTm, int totalSlots,
                             int jobParallelism, boolean slotSufficient) {
            this.taskManagers = taskManagers;
            this.slotsPerTm = slotsPerTm;
            this.totalSlots = totalSlots;
            this.jobParallelism = jobParallelism;
            this.slotSufficient = slotSufficient;
        }
    }

    public static class OperatorPressure {
        public final String name;
        public long busyMs;
        public long backpressuredMs;
        public final long idleMs;

        public OperatorPressure(String name, long busyMs, long backpressuredMs, long idleMs) {
            this.name = name;
            this.busyMs = busyMs;
            this.backpressuredMs = backpressuredMs;
            this.idleMs = idleMs;
        }
    }

    public static class BackpressurePropagation {
        public final List<OperatorPressure> operators;

        public BackpressurePropagation(List<OperatorPressure> operators) {
            this.operators = operators;
        }

        public boolean upstreamFeelsBackpressure() {
            return operators.size() > 1 && operators.get(0).backpressuredMs > 0;
        }
    }
}
