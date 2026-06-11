package org.example.job.savepoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 纯逻辑模拟：Savepoint 恢复前校验算子 UID、状态描述符、并行度重分配。
 * 供单测使用，无需启动 MiniCluster。
 */
public final class SavepointRestoreValidator {

    private SavepointRestoreValidator() {
    }

    public static RestoreResult validate(
            SavepointMetadata savepoint,
            JobTopology newTopology,
            boolean allowNonRestoredState) {

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        Set<String> newUids = newTopology.operatorUids();
        for (String oldUid : savepoint.operatorUids()) {
            if (!newUids.contains(oldUid)) {
                String msg = "Savepoint 中存在算子 UID=" + oldUid + "，新拓扑未找到匹配项";
                if (allowNonRestoredState) {
                    warnings.add(msg + "（allowNonRestoredState=true，将丢弃该算子状态）");
                } else {
                    errors.add(msg + " → 恢复失败");
                }
            }
        }

        for (StateDescriptorSnapshot snap : savepoint.stateDescriptors()) {
            StateDescriptorSnapshot current = newTopology.stateDescriptor(snap.operatorUid, snap.name);
            if (current == null) {
                String msg = "算子 " + snap.operatorUid + " 的状态描述符 " + snap.name + " 在新拓扑中不存在";
                if (allowNonRestoredState) {
                    warnings.add(msg);
                } else {
                    errors.add(msg);
                }
                continue;
            }
            if (!snap.isCompatibleWith(current)) {
                errors.add("状态不兼容："
                        + snap.operatorUid + "/" + snap.name
                        + " savepoint=" + snap.valueType + " job=" + current.valueType);
            }
        }

        if (savepoint.parallelism() != newTopology.parallelism()) {
            warnings.add("并行度 " + savepoint.parallelism() + " → " + newTopology.parallelism()
                    + "：Keyed State 将按 keyGroup 重分配（maxParallelism 不变时可扩缩容）");
        }

        return new RestoreResult(errors.isEmpty(), errors, warnings);
    }

    /** 模拟 keyGroup 在并行度变更时的重分配（简化：hash(key) % maxParallelism → subtask） */
    public static int resolveSubtask(String key, int parallelism, int maxParallelism) {
        int keyGroup = Math.floorMod(key.hashCode(), maxParallelism);
        return keyGroup * parallelism / maxParallelism;
    }

    public static class SavepointMetadata {
        private final int parallelism;
        private final Set<String> operatorUids = new HashSet<>();
        private final List<StateDescriptorSnapshot> stateDescriptors = new ArrayList<>();

        public SavepointMetadata(int parallelism) {
            this.parallelism = parallelism;
        }

        public SavepointMetadata addOperator(String uid) {
            operatorUids.add(uid);
            return this;
        }

        public SavepointMetadata addState(String operatorUid, String name, String keyType, String valueType) {
            stateDescriptors.add(new StateDescriptorSnapshot(operatorUid, name, keyType, valueType));
            return this;
        }

        public int parallelism() {
            return parallelism;
        }

        public Set<String> operatorUids() {
            return operatorUids;
        }

        public List<StateDescriptorSnapshot> stateDescriptors() {
            return stateDescriptors;
        }
    }

    public static class JobTopology {
        private final int parallelism;
        private final int maxParallelism;
        private final Set<String> operatorUids = new HashSet<>();
        private final Map<String, StateDescriptorSnapshot> descriptors = new HashMap<>();

        public JobTopology(int parallelism, int maxParallelism) {
            this.parallelism = parallelism;
            this.maxParallelism = maxParallelism;
        }

        public JobTopology addOperator(String uid) {
            operatorUids.add(uid);
            return this;
        }

        public JobTopology addState(String operatorUid, String name, String keyType, String valueType) {
            descriptors.put(descriptorKey(operatorUid, name),
                    new StateDescriptorSnapshot(operatorUid, name, keyType, valueType));
            return this;
        }

        public int parallelism() {
            return parallelism;
        }

        public Set<String> operatorUids() {
            return operatorUids;
        }

        public StateDescriptorSnapshot stateDescriptor(String operatorUid, String name) {
            return descriptors.get(descriptorKey(operatorUid, name));
        }

        private static String descriptorKey(String operatorUid, String name) {
            return operatorUid + "#" + name;
        }
    }

    public static class StateDescriptorSnapshot {
        public final String operatorUid;
        public final String name;
        public final String keyType;
        public final String valueType;

        public StateDescriptorSnapshot(String operatorUid, String name, String keyType, String valueType) {
            this.operatorUid = operatorUid;
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public boolean isCompatibleWith(StateDescriptorSnapshot other) {
            return Objects.equals(name, other.name)
                    && Objects.equals(keyType, other.keyType)
                    && Objects.equals(valueType, other.valueType);
        }
    }

    public static class RestoreResult {
        public final boolean success;
        public final List<String> errors;
        public final List<String> warnings;

        public RestoreResult(boolean success, List<String> errors, List<String> warnings) {
            this.success = success;
            this.errors = errors;
            this.warnings = warnings;
        }
    }
}
