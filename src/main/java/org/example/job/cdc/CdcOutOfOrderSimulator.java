package org.example.job.cdc;

import org.example.dto.CdcChangeEvent;
import org.example.dto.CdcEnrollmentSnapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 纯内存模拟：乱序 CDC 事件经 LWW 后的最终状态。
 */
public final class CdcOutOfOrderSimulator {

    private CdcOutOfOrderSimulator() {
    }

    public static CdcEnrollmentSnapshot applyLastWriteWins(
            List<CdcChangeEvent> events,
            String versionStrategy) {

        CdcEnrollmentSnapshot latest = null;
        long maxVersion = Long.MIN_VALUE;

        for (CdcChangeEvent event : events) {
            CdcVersionResolver.VersionedEvent ve = CdcVersionResolver.resolve(event, versionStrategy);
            if (ve.version >= maxVersion) {
                maxVersion = ve.version;
                latest = toSnapshot(ve);
            }
        }
        return latest;
    }

    public static List<CdcChangeEvent> sortByArrival(List<CdcChangeEvent> events) {
        List<CdcChangeEvent> copy = new ArrayList<>(events);
        copy.sort(Comparator.comparingLong(CdcChangeEvent::getEventTsMs));
        return copy;
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
