package com.iota.iri.network.gossip;

import org.slf4j.Logger;

public class StageTime {

    private long[] deltas;
    private int nextDelta = 0;
    private int printEvery;
    private String stageName;

    public StageTime(String stageName, int deltaCount, int printEvery) {
        deltas = new long[deltaCount];
        this.printEvery = printEvery;
        this.stageName = stageName;
    }

    private long lastTs = System.currentTimeMillis();
    private long startPoint;

    public void startPoint() {
        startPoint = System.currentTimeMillis();
    }

    public void endPoint(Logger log) {
        long now = System.currentTimeMillis();
        long delta = now - startPoint;
        deltas[nextDelta] = delta;
        nextDelta++;
        if (nextDelta == deltas.length) {
            nextDelta = 0;
        }
        if (startPoint - lastTs > printEvery) {
            lastTs = now;
            long avg = 0;
            for (long d : deltas) {
                avg += d;
            }
            avg /= deltas.length;
            log.info("average {} stage time {}ms", stageName, avg);
        }
    }

}
