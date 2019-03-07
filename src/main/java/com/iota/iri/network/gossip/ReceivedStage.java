package com.iota.iri.network.gossip;

import com.iota.iri.TransactionValidator;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReceivedStage implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReceivedStage.class);

    private Tangle tangle;
    private TransactionValidator txValidator;
    private SnapshotProvider snapshotProvider;
    private BlockingQueue<Pair<Peer, TransactionViewModel>> receivedStageQueue;
    private BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue;

    public ReceivedStage(
            BlockingQueue<Pair<Peer, TransactionViewModel>> receivedStageQueue,
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
                Pair<Peer, TransactionViewModel> tuple = receivedStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                Peer peer = tuple.getLeft();
                TransactionViewModel tvm = tuple.getRight();

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
                        if(peer != null){
                            tvm.updateSender(peer.getId());
                        }
                        tvm.update(tangle, snapshotProvider.getInitialSnapshot(), "arrivalTime|sender");
                    } catch (Exception e) {
                        log.error("error updating newly received tx", e);
                    }

                    // broadcast the newly saved tx to the other peers
                    broadcastStageQueue.put(new ImmutablePair<>(peer, tvm));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("received stage stopped");
    }
}
