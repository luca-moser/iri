package com.iota.iri.network.gossip;

import com.iota.iri.conf.NodeConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.storage.Tangle;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReplyStage implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ReplyStage.class);

    private Gossip gossip;
    private Tangle tangle;
    private NodeConfig config;
    private TransactionRequester txRequester;
    private TipsViewModel tipsViewModel;
    private LatestMilestoneTracker latestMilestoneTracker;
    private PreProcessStage preProcessStage;
    private static final SecureRandom rnd = new SecureRandom();

    private BlockingQueue<Pair<Peer, Hash>> replyStageQueue;

    public ReplyStage(
            ArrayBlockingQueue<Pair<Peer, Hash>> replyStageQueue,
            Gossip gossip, NodeConfig config, Tangle tangle,
            TipsViewModel tipsViewModel, LatestMilestoneTracker latestMilestoneTracker,
            PreProcessStage preProcessStage, TransactionRequester txRequester
    ) {
        this.replyStageQueue = replyStageQueue;
        this.gossip = gossip;
        this.config = config;
        this.tangle = tangle;
        this.tipsViewModel = tipsViewModel;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.preProcessStage = preProcessStage;
        this.txRequester = txRequester;
    }

    @Override
    public void run() {
        log.info("reply stage ready");
        while (!Gossip.SHUTDOWN.get()) {
            try {
                Pair<Peer, Hash> tuple = replyStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                Peer peer = tuple.getLeft();
                Hash requestedHash = tuple.getRight();

                TransactionViewModel tvm = null;
                Hash transactionPointer;

                if (requestedHash.equals(Hash.NULL_HASH)) {
                    try {
                        // retrieve random tx
                        if (txRequester.numberOfTransactionsToRequest() <= 0 || rnd.nextDouble() >= config.getpReplyRandomTip()) {
                            return;
                        }
                        transactionPointer = getRandomTipPointer();
                        tvm = TransactionViewModel.fromHash(tangle, transactionPointer);
                    } catch (Exception e) {
                        log.error("error loading random tip for reply", e);
                    }
                } else {
                    try {
                        // retrieve requested tx
                        tvm = TransactionViewModel.fromHash(tangle, HashFactory.TRANSACTION.create(requestedHash.bytes(), 0, PreProcessStage.REQ_HASH_SIZE));
                    } catch (Exception e) {
                        log.error("error while searching for explicitly asked for tx", e);
                    }
                }

                if (tvm != null && tvm.getType() == TransactionViewModel.FILLED_SLOT) {
                    try {
                        // send the requested tx data to the requester
                        gossip.sendPacket(peer, tvm);
                        // cache the replied with tx
                        long txDigest = Gossip.getTxCacheDigest(tvm.getBytes());
                        preProcessStage.cacheHash(txDigest, tvm.getHash());
                    } catch (Exception e) {
                        log.error("error fetching transaction to request.", e);
                    }
                    continue;
                }

                if (requestedHash.equals(Hash.NULL_HASH) || rnd.nextDouble() >= config.getpPropagateRequest()) {
                    continue;
                }

                try {
                    // we don't have the requested tx, so we add it to our own request queue
                    txRequester.requestTransaction(requestedHash, false);
                } catch (Exception e) {
                    log.error("error adding requested tx to own request queue", e);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("reply stage stopped");
    }

    private Hash getRandomTipPointer() throws Exception {
        Hash tip = rnd.nextDouble() < config.getpSendMilestone() ? latestMilestoneTracker.getLatestMilestoneHash() : tipsViewModel.getRandomSolidTipHash();
        return tip == null ? Hash.NULL_HASH : tip;
    }
}
