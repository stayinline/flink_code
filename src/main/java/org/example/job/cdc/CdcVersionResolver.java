package org.example.job.cdc;

import org.example.dto.CdcChangeEvent;

/**
 * 单调版本号解析：用于同主键乱序到达时的 last-write-wins。
 * <p>
 * 优先级：binlog file+pos &gt; Debezium ts_ms &gt; 业务 updated_at（须确认 DB 单调）。
 */
public final class CdcVersionResolver {

    public static final String SOURCE_BINLOG = "binlog_pos";
    public static final String SOURCE_DEBEZIUM_TS = "debezium_ts_ms";
    public static final String SOURCE_DB_UPDATED_AT = "db_updated_at";

    private CdcVersionResolver() {
    }

    public static VersionedEvent resolve(CdcChangeEvent event, String strategy) {
        if (SOURCE_DB_UPDATED_AT.equalsIgnoreCase(strategy)
                && event.getDbUpdatedAtMs() != null && event.getDbUpdatedAtMs() > 0) {
            return new VersionedEvent(event.getDbUpdatedAtMs(), SOURCE_DB_UPDATED_AT, event);
        }
        if (event.getBinlogFile() != null && event.getBinlogPos() != null) {
            long version = binlogVersion(event.getBinlogFile(), event.getBinlogPos());
            return new VersionedEvent(version, SOURCE_BINLOG, event);
        }
        return new VersionedEvent(event.getEventTsMs(), SOURCE_DEBEZIUM_TS, event);
    }

    /** file 序号 * 1e9 + pos，保证同集群内单调（简化版） */
    public static long binlogVersion(String binlogFile, long pos) {
        int fileNum = 0;
        int idx = binlogFile.lastIndexOf('.');
        if (idx >= 0) {
            try {
                fileNum = Integer.parseInt(binlogFile.substring(idx + 1));
            } catch (NumberFormatException ignored) {
                fileNum = Math.abs(binlogFile.hashCode() % 10000);
            }
        }
        return (long) fileNum * 1_000_000_000L + pos;
    }

    public static class VersionedEvent {
        public final long version;
        public final String source;
        public final CdcChangeEvent event;

        public VersionedEvent(long version, String source, CdcChangeEvent event) {
            this.version = version;
            this.source = source;
            this.event = event;
        }
    }
}
