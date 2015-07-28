package ru.dsi.bgbilling.modules.inet.accounting.quota;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Элемент очереди слайсов
 */
public class Slice{
    /**
     * Конец временного интервала этого слайса
     */
    public final long endTime;

    /**
     * Объём трафика слайса
     */
    public final AtomicLong amount;

    public Slice(long amount, long endTime) {
        this.endTime = endTime;
        this.amount = new AtomicLong(amount);
    }
}