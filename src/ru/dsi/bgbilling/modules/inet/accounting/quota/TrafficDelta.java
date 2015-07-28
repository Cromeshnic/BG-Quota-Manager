package ru.dsi.bgbilling.modules.inet.accounting.quota;

/**
 * Объём трафика и период времени (в мс), за который он был потреблён
 */
public class TrafficDelta {
    public final long amount;
    public final long start;
    public final long end;

    public TrafficDelta(long amount, long start, long end) {
        this.amount = amount;
        this.start = start;
        this.end = end;
    }
}
