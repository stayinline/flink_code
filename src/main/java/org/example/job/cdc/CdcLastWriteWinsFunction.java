package org.example.job.cdc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.CdcChangeEvent;
import org.example.dto.CdcEnrollmentSnapshot;

/**
 * 同主键 CDC 更新乱序到达时，按单调版本 last-write-wins。
 */
public class CdcLastWriteWinsFunction
        extends KeyedProcessFunction<Long, CdcChangeEvent, CdcEnrollmentSnapshot> {

    private final String versionStrategy;

    private transient ValueState<Long> maxVersionState;

    public CdcLastWriteWinsFunction(String versionStrategy) {
        this.versionStrategy = versionStrategy != null ? versionStrategy : CdcVersionResolver.SOURCE_BINLOG;
    }

    @Override
    public void open(Configuration parameters) {
        maxVersionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cdc-lww-max-version", Long.class));
    }

    @Override
    public void processElement(CdcChangeEvent event, Context ctx, Collector<CdcEnrollmentSnapshot> out)
            throws Exception {
        CdcVersionResolver.VersionedEvent ve = CdcVersionResolver.resolve(event, versionStrategy);
        Long prevMax = maxVersionState.value();
        long prev = prevMax != null ? prevMax : Long.MIN_VALUE;

        if (ve.version < prev) {
            System.out.printf(
                    "[LWW-SKIP] pk=%d incoming=%d < max=%d | status=%s op=%s | 旧值不能覆盖新值%n",
                    event.getPrimaryKeyId(), ve.version, prev,
                    event.getEnrollStatus(), event.getOp());
            return;
        }

        maxVersionState.update(ve.version);
        CdcEnrollmentSnapshot snap = toSnapshot(ve);
        System.out.printf(
                "[LWW-ACCEPT] pk=%d version=%d src=%s op=%s status=%s deleted=%s%n",
                snap.getPrimaryKeyId(), snap.getVersion(), snap.getVersionSource(),
                snap.getLastOp(), snap.getEnrollStatus(), snap.isDeleted());
        out.collect(snap);
    }

    private static CdcEnrollmentSnapshot toSnapshot(CdcVersionResolver.VersionedEvent ve) {
        CdcChangeEvent e = ve.event;
        CdcEnrollmentSnapshot snap = new CdcEnrollmentSnapshot();
        snap.setPrimaryKeyId(e.getPrimaryKeyId());
        snap.setStudentId(e.getStudentId());
        snap.setCourseId(e.getCourseId());
        snap.setEnrollStatus(e.getEnrollStatus());
        snap.setDeleted(e.isDelete());
        snap.setVersion(ve.version);
        snap.setVersionSource(ve.source);
        snap.setEventTsMs(e.getEventTsMs());
        snap.setLastOp(e.getOp());
        return snap;
    }
}
