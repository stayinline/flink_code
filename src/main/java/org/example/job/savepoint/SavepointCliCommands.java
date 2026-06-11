package org.example.job.savepoint;

/**
 * 集群模式 Savepoint 触发 / 恢复 / 回滚 CLI 命令模板（Flink 1.14）。
 */
public final class SavepointCliCommands {

    private SavepointCliCommands() {
    }

    public static String triggerSavepoint(String jobId, String targetDir) {
        return String.format(
                "flink savepoint %s %s",
                jobId,
                targetDir != null ? targetDir : SavepointConfigurator.SAVEPOINT_BASE_DIR);
    }

    public static String stopWithSavepoint(String jobId, String targetDir) {
        return String.format(
                "flink stop --savepointPath %s %s",
                targetDir != null ? targetDir : SavepointConfigurator.SAVEPOINT_BASE_DIR,
                jobId);
    }

    public static String runFromSavepoint(String jarPath, String savepointPath, boolean allowNonRestored) {
        StringBuilder sb = new StringBuilder("flink run");
        if (allowNonRestored) {
            sb.append(" -n");
        }
        sb.append(" -s ").append(savepointPath);
        sb.append(" ").append(jarPath);
        sb.append(" v2 4 hashmap");
        return sb.toString();
    }

    public static String cancelRetainCheckpoint(String jobId) {
        return "flink cancel -s " + SavepointConfigurator.CHECKPOINT_BASE_DIR + " " + jobId
                + "  # Flink 1.14+ 外部化 CK；Savepoint 仍推荐用于发布";
    }

    public static void printReleasePlaybook(String jobId) {
        String spDir = SavepointConfigurator.SAVEPOINT_BASE_DIR;
        System.out.println("--- Savepoint 发布 playbook（集群）---");
        System.out.println("1) 触发 Savepoint 并停止：");
        System.out.println("   " + stopWithSavepoint(jobId, spDir));
        System.out.println("2) 部署新版本 JAR，从 Savepoint 恢复（兼容升级 V1→V2）：");
        System.out.println("   flink run -s <savepoint-path> target/flink_code-1.0-SNAPSHOT.jar v2 4 hashmap");
        System.out.println("3) 并行度调整（例 2→4，UID 不变）：");
        System.out.println("   flink run -s <savepoint-path> target/flink_code-1.0-SNAPSHOT.jar v2 4 hashmap");
        System.out.println("4) 异常回滚到旧版本：");
        System.out.println("   flink run -s <savepoint-path> target/flink_code-1.0-SNAPSHOT.jar v1 2 hashmap");
        System.out.println("5) 删除有状态算子后恢复（慎用）：");
        System.out.println("   flink run -n -s <savepoint-path> ...   # allowNonRestoredState");
        System.out.println("6) 对比：Checkpoint 自动触发，面向故障恢复；勿直接拿 CK 路径做版本发布");
        System.out.println("   UI → Checkpoints 可看最近 CK；发布请用 stop --savepointPath");
        System.out.println("-----------------------------------");
    }
}
