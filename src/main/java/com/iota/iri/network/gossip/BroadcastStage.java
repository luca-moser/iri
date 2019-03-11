package com.iota.iri.network.gossip;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.utils.Converter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BroadcastStage implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BroadcastStage.class);

    private Gossip gossip;
    private BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue;

    public BroadcastStage(BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue, Gossip gossip) {
        this.broadcastStageQueue = broadcastStageQueue;
        this.gossip = gossip;
    }

    @Override
    public void run() {
        log.info("broadcast stage ready");
        while (!Gossip.SHUTDOWN.get()) {
            try {
                Pair<Peer, TransactionViewModel> tuple = broadcastStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                Peer originPeer = tuple.getLeft();
                TransactionViewModel tvm = tuple.getRight();

                // racy
                Map<String, Peer> currentlyConnectedPeers = gossip.getPeers();
                for (Peer peer : currentlyConnectedPeers.values()) {
                    // don't send back to origin peer
                    if (peer.equals(originPeer)) {
                        continue;
                    }

                    gossip.sendPacket(peer, tvm);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("broadcast stage stopped");
    }
}
