package com.iota.iri.network.pipeline;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.network.NeighborRouter;
import com.iota.iri.network.neighbor.Neighbor;
import com.iota.iri.service.warpsync.WarpSyncer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * The {@link BroadcastStage} takes care of broadcasting newly received transactions to all neighbors except the
 * neighbor from which the transaction originated from.
 */
public class BroadcastStage {

    private static final Logger log = LoggerFactory.getLogger(BroadcastStage.class);

    private NeighborRouter neighborRouter;

    /**
     * Creates a new {@link BroadcastStage}.
     * 
     * @param neighborRouter The {@link NeighborRouter} instance to use to broadcast
     */
    public BroadcastStage(NeighborRouter neighborRouter) {
        this.neighborRouter = neighborRouter;
    }

    /**
     * Extracts the transaction and then broadcasts it to all neighbors. If the transaction originated from a neighbor,
     * it is not sent to that given neighbor.
     * 
     * @param ctx the broadcast stage {@link ProcessingContext}
     * @return the same ctx as passed in
     */
    public ProcessingContext process(ProcessingContext ctx) {
        BroadcastPayload payload = (BroadcastPayload) ctx.getPayload();
        Optional<Neighbor> optOriginNeighbor = payload.getOriginNeighbor();
        TransactionViewModel tvm = payload.getTransactionViewModel();

        // don't broadcast anything while the node is still warp syncing
        if (WarpSyncer.IS_WARP_SYNCING.get()) {
            return ctx;
        }

        // racy
        Map<String, Neighbor> currentlyConnectedNeighbors = neighborRouter.getConnectedNeighbors();
        for (Neighbor neighbor : currentlyConnectedNeighbors.values()) {
            // don't send back to origin neighbor
            if (optOriginNeighbor.isPresent() && neighbor.equals(optOriginNeighbor.get())) {
                continue;
            }
            try {
                neighborRouter.gossipTransactionTo(neighbor, tvm);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }

        return ctx;
    }
}
