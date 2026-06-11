package org.example.job.runtime;

/**
 * 估算逻辑 Subtask / 物理 Task 数量，帮助理解「并行度 ≠ Slot 数」与 Operator Chain 取舍。
 * <p>
 * 简化模型：本 Demo 拓扑 Source→Map→Probe → keyBy → Window+Aggregate → Sink。
 */
public final class RuntimeTopologyEstimator {

    private RuntimeTopologyEstimator() {
    }

    public static TopologyEstimate estimate(RuntimeConfigurator.RuntimeOptions options) {
        int p = options.parallelism;
        boolean chainDisabled = RuntimeConfigurator.isChainDisabled(options);
        boolean isolateSink = RuntimeConfigurator.isolateSinkSlot(options);

        // keyBy 必然打断 chain：前后各一段
        int chainedSegmentsPerPipeline = chainDisabled ? 4 : 2;
        // Source+Map+Probe | WindowAgg+Formatter 两段；nochain 时每算子独立 Task
        int tasksPerPipeline = chainDisabled ? 5 : 2;
        int subtasksPerOperator = p;
        int totalSubtasks = subtasksPerOperator * (chainDisabled ? 5 : 3);
        // Source chain(3 ops) + Window chain(2 ops) + Sink = 3 logical operators with chain on
        if (chainDisabled) {
            totalSubtasks = p * 5;
        } else {
            totalSubtasks = p * 3;
        }

        int minSlotsWithSharing = p;
        int minSlotsWithIsolation = isolateSink ? p * 2 : p;

        return new TopologyEstimate(
                p,
                tasksPerPipeline,
                chainedSegmentsPerPipeline,
                totalSubtasks,
                minSlotsWithSharing,
                minSlotsWithIsolation,
                chainDisabled,
                isolateSink
        );
    }

    public static void printEstimate(TopologyEstimate est) {
        System.out.println("--- Runtime 拓扑估算（逻辑 vs 物理）---");
        System.out.printf("  作业并行度 P=%d → 每个算子 %d 个 Subtask%n", est.parallelism, est.parallelism);
        System.out.printf("  Operator Chain: %s → 约 %d 个 Task 链段/流水线（每段含 1~3 个算子）%n",
                est.chainDisabled ? "禁用（每算子独立 Task）" : "启用（keyBy 前/后各 chain）",
                est.tasksPerPipeline);
        System.out.printf("  估算 Subtask 总数: ~%d（UI → Task Managers → Subtasks）%n", est.totalSubtasks);
        System.out.printf("  Slot 共享时最少 Slot: ~%d | Sink 独立 SlotSharingGroup 时: ~%d%n",
                est.minSlotsWithSharing, est.minSlotsWithIsolation);
        System.out.println("  逻辑并行度 P 与物理 Slot 数不是一回事：Slot 由 TM 配置，Subtask 由 P 决定");
        System.out.println("--------------------------------------");
    }

    public static class TopologyEstimate {
        public final int parallelism;
        public final int tasksPerPipeline;
        public final int chainedSegmentsPerPipeline;
        public final int totalSubtasks;
        public final int minSlotsWithSharing;
        public final int minSlotsWithIsolation;
        public final boolean chainDisabled;
        public final boolean isolateSink;

        public TopologyEstimate(int parallelism, int tasksPerPipeline, int chainedSegmentsPerPipeline,
                                int totalSubtasks, int minSlotsWithSharing, int minSlotsWithIsolation,
                                boolean chainDisabled, boolean isolateSink) {
            this.parallelism = parallelism;
            this.tasksPerPipeline = tasksPerPipeline;
            this.chainedSegmentsPerPipeline = chainedSegmentsPerPipeline;
            this.totalSubtasks = totalSubtasks;
            this.minSlotsWithSharing = minSlotsWithSharing;
            this.minSlotsWithIsolation = minSlotsWithIsolation;
            this.chainDisabled = chainDisabled;
            this.isolateSink = isolateSink;
        }
    }
}
