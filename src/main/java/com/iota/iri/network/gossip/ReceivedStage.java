package com.iota.iri.network.gossip;

import com.iota.iri.TransactionValidator;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReceivedStage implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReceivedStage.class);

    private Tangle tangle;
    private TransactionValidator txValidator;
    private SnapshotProvider snapshotProvider;
    private BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> receivedStageQueue;
    private BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue;

    public ReceivedStage(
            BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> receivedStageQueue,
            BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue,
            Tangle tangle, TransactionValidator txValidator, SnapshotProvider snapshotProvider) {
        this.receivedStageQueue = receivedStageQueue;
        this.broadcastStageQueue = broadcastStageQueue;
        this.txValidator = txValidator;
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
    }

    @Override
    public void run() {
        log.info("received stage ready");
        while (!Gossip.SHUTDOWN.get()) {
            try {
                Triple<Peer, TransactionViewModel, Boolean> triple = receivedStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (triple == null) {
                    continue;
                }
                Peer peer = triple.getLeft();
                TransactionViewModel tvm = triple.getMiddle();
                boolean bypass = triple.getRight();

                boolean stored;
                try {
                    stored = tvm.store(tangle, snapshotProvider.getInitialSnapshot());
                } catch (Exception e) {
                    log.error("error persisting newly received tx", e);
                    continue;
                }

                if (stored) {
                    tvm.setArrivalTime(System.currentTimeMillis());
                    try {
                        txValidator.updateStatus(tvm);
                        // peer might be null because tx came from a broadcastTransaction command
                        if (peer != null) {
                            tvm.updateSender(peer.getId());
                        }
                        tvm.update(tangle, snapshotProvider.getInitialSnapshot(), "arrivalTime|sender");
                    } catch (Exception e) {
                        log.error("error updating newly received tx", e);
                    }
                }

                // broadcast the newly saved tx to the other peers
                broadcastStageQueue.put(new ImmutablePair<>(peer, tvm));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("received stage stopped");
    }
}
