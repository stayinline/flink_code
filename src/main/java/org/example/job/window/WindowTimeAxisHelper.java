package org.example.job.window;

import java.util.ArrayList;
import java.util.List;

/**
 * 纯计算工具：给定事件 ts 与 Sliding/Tumbling 参数，列出该事件归属的窗口 [start, end)。
 * alignMs 默认 0，与 Flink EventTime 窗口 epoch 对齐一致。
 */
public final class WindowTimeAxisHelper {

    private WindowTimeAxisHelper() {
    }

    public static List<long[]> tumblingWindowsContaining(long eventTsMs, long sizeMs, long alignMs) {
        List<long[]> windows = new ArrayList<>();
        long start = alignMs + ((eventTsMs - alignMs) / sizeMs) * sizeMs;
        if (eventTsMs >= start && eventTsMs < start + sizeMs) {
            windows.add(new long[]{start, start + sizeMs});
        }
        return windows;
    }

    public static List<long[]> slidingWindowsContaining(long eventTsMs, long sizeMs, long slideMs, long alignMs) {
        List<long[]> windows = new ArrayList<>();
        if (slideMs <= 0 || sizeMs <= 0) {
            return windows;
        }
        long kMin = (long) Math.ceil((double) (eventTsMs - sizeMs + 1 - alignMs) / slideMs);
        long kMax = (long) Math.floor((double) (eventTsMs - alignMs) / slideMs);
        for (long k = kMin; k <= kMax; k++) {
            long start = alignMs + k * slideMs;
            if (eventTsMs >= start && eventTsMs < start + sizeMs) {
                windows.add(new long[]{start, start + sizeMs});
            }
        }
        return windows;
    }

    /** 窗口数量膨胀估算：在 [0, horizonMs) 内 slide 越小窗口副本越多 */
    public static int slidingWindowCount(long horizonMs, long sizeMs, long slideMs) {
        if (slideMs <= 0 || horizonMs <= sizeMs) {
            return horizonMs > 0 && sizeMs > 0 ? 1 : 0;
        }
        return (int) ((horizonMs - sizeMs) / slideMs) + 1;
    }
}
