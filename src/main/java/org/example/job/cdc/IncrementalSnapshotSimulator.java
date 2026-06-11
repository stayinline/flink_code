package org.example.job.cdc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 简化模拟 DBLog / Netflix 无锁增量快照：chunk 切分 + 高低水位 binlog 合并。
 * 供单测理解「不锁表、可并行、不断点丢数」原理，非真实 Debezium 实现。
 */
public final class IncrementalSnapshotSimulator {

    private IncrementalSnapshotSimulator() {
    }

    /**
     * @param tableRows     表当前全量数据（pk → row）
     * @param binlogEvents  快照期间并发产生的 binlog 变更（按 binlogOffset 排序）
     * @param chunkBounds   主键 chunk 边界 [low, high]
     */
    public static List<Row> runIncrementalSnapshot(
            Map<Long, Row> tableRows,
            List<BinlogEvent> binlogEvents,
            List<long[]> chunkBounds) {

        List<Row> result = new ArrayList<>();
        List<BinlogEvent> sortedBinlog = binlogEvents.stream()
                .sorted(Comparator.comparingLong(BinlogEvent::getOffset))
                .collect(Collectors.toList());

        for (long[] bound : chunkBounds) {
            long low = bound[0];
            long high = bound[1];
            long lowWatermark = findWatermarkBefore(sortedBinlog, low);
            long highWatermark = findWatermarkAfter(sortedBinlog, high);

            Map<Long, Row> chunkSnapshot = snapshotChunk(tableRows, low, high);
            Map<Long, Row> merged = mergeWithBinlogWindow(chunkSnapshot, sortedBinlog, lowWatermark, highWatermark);
            result.addAll(merged.values());
        }
        return result.stream()
                .sorted(Comparator.comparingLong(Row::getId))
                .collect(Collectors.toList());
    }

    private static Map<Long, Row> snapshotChunk(Map<Long, Row> table, long low, long high) {
        Map<Long, Row> snap = new HashMap<>();
        for (Map.Entry<Long, Row> e : table.entrySet()) {
            long pk = e.getKey();
            if (pk >= low && pk <= high) {
                snap.put(pk, e.getValue().copy());
            }
        }
        return snap;
    }

    private static Map<Long, Row> mergeWithBinlogWindow(
            Map<Long, Row> snapshot,
            List<BinlogEvent> binlog,
            long lowWm,
            long highWm) {

        Map<Long, Row> merged = new HashMap<>(snapshot);
        for (BinlogEvent evt : binlog) {
            if (evt.offset <= lowWm || evt.offset > highWm) {
                continue;
            }
            if ("d".equals(evt.op)) {
                merged.remove(evt.row.getId());
            } else {
                merged.put(evt.row.getId(), evt.row.copy());
            }
        }
        return merged;
    }

    private static long findWatermarkBefore(List<BinlogEvent> binlog, long pkLow) {
        return binlog.stream()
                .filter(e -> e.row.getId() < pkLow)
                .mapToLong(BinlogEvent::getOffset)
                .max()
                .orElse(0L);
    }

    private static long findWatermarkAfter(List<BinlogEvent> binlog, long pkHigh) {
        return binlog.stream()
                .filter(e -> e.row.getId() <= pkHigh)
                .mapToLong(BinlogEvent::getOffset)
                .max()
                .orElse(0L);
    }

    public static class Row {
        private final long id;
        private final String studentId;
        private final String status;

        public Row(long id, String studentId, String status) {
            this.id = id;
            this.studentId = studentId;
            this.status = status;
        }

        public long getId() {
            return id;
        }

        public String getStudentId() {
            return studentId;
        }

        public String getStatus() {
            return status;
        }

        public Row copy() {
            return new Row(id, studentId, status);
        }
    }

    public static class BinlogEvent {
        private final long offset;
        private final String op;
        private final Row row;

        public BinlogEvent(long offset, String op, Row row) {
            this.offset = offset;
            this.op = op;
            this.row = row;
        }

        public long getOffset() {
            return offset;
        }
    }
}
