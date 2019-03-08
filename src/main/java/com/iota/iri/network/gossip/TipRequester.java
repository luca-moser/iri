package com.iota.iri.network.gossip;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TipRequester implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TipRequester.class);

    private Gossip gossip;
    private Tangle tangle;
    private TransactionRequester txRequester;
    private LatestMilestoneTracker latestMilestoneTracker;

    public TipRequester(
            Gossip gossip, Tangle tangle,
            LatestMilestoneTracker latestMilestoneTracker,
            TransactionRequester txRequester
    ) {
        this.gossip = gossip;
        this.tangle = tangle;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.txRequester = txRequester;
    }

    @Override
    public void run() {
        log.info("tips requester ready");

        long lastTime = 0;
        while (!Gossip.SHUTDOWN.get()) {
            try {
                final TransactionViewModel msTVM = TransactionViewModel.fromHash(tangle, latestMilestoneTracker.getLatestMilestoneHash());

                if(msTVM.getBytes().length > 0){
                    gossip.getPeers().values().forEach(peer -> {
                        try {
                            gossip.sendPacket(peer, msTVM, true);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }

                long now = System.currentTimeMillis();
                if ((now - lastTime) > 10000L) {
                    lastTime = now;
                    log.info("toProcess = {} , toBroadcast = {} , toRequest = {} , toReply = {} / totalTransactions = {}",
                            gossip.getReceivedStageQueue().size(), gossip.getBroadcastStageQueue().size(),
                            txRequester.numberOfTransactionsToRequest(), gossip.getReplyStageQueue().size(),
                            TransactionViewModel.getNumberOfStoredTransactions(tangle));
                }

                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (final Exception e) {
                log.error("Tips Requester Thread Exception:", e);
            }
        }
        log.info("tips requester stopped");
    }
}
