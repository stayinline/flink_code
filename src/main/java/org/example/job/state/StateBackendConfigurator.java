package org.example.job.state;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * HashMapStateBackend vs EmbeddedRocksDBStateBackend 配置入口。
 */
public final class StateBackendConfigurator {

    public static final String BACKEND_HASHMAP = "hashmap";
    public static final String BACKEND_ROCKSDB = "rocksdb";

    private StateBackendConfigurator() {
    }

    public static String resolveBackend(String[] args) {
        if (args != null && args.length > 0 && !args[0].isBlank()) {
            return args[0].trim().toLowerCase();
        }
        return System.getProperty("state.backend", BACKEND_HASHMAP).trim().toLowerCase();
    }

    public static void configure(StreamExecutionEnvironment env, String backend) {
        if (BACKEND_ROCKSDB.equals(backend)) {
            configureRocksDb(env);
        } else {
            configureHashMap(env);
        }
    }

    private static void configureHashMap(StreamExecutionEnvironment env) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/flink-state-demo-checkpoints/hashmap"));
    }

    private static void configureRocksDb(StreamExecutionEnvironment env) {
        // 增量 Checkpoint：仅上传变更的 SST，大状态场景显著降低 checkpoint 耗时
        EmbeddedRocksDBStateBackend rocksDb = new EmbeddedRocksDBStateBackend(true);

        // 调优项 1：预定义选项（HDD 高内存场景模板，含 block cache / write buffer 基线）
        rocksDb.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);

        env.setStateBackend(rocksDb);
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/flink-state-demo-checkpoints/rocksdb"));

        // 调优项 2：Managed Memory，RocksDB 与 Flink 共享 TM 堆外内存池
        env.getConfig().set(org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE,
                MemorySize.parse("256m"));

        // 调优项 3：单并发 Checkpoint，大状态避免重叠 checkpoint 打满磁盘 IO
        env.getCheckpointConfig().setCheckpointingMode(CheckpointConfig.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    public static String describeBackend(String backend) {
        if (BACKEND_ROCKSDB.equals(backend)) {
            return "EmbeddedRocksDBStateBackend（磁盘、可超内存、增量 Checkpoint、有序列化开销）";
        }
        return "HashMapStateBackend（内存、快、受 JVM heap 限制）";
    }
}
