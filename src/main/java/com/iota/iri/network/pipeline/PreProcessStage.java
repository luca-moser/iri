package com.iota.iri.network.pipeline;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.persistables.Transaction;
import com.iota.iri.network.FIFOCache;
import com.iota.iri.network.NeighborRouter;
import com.iota.iri.network.protocol.Protocol;
import com.iota.iri.utils.Converter;

import java.nio.ByteBuffer;

/**
 * The {@link PreProcessStage} expands truncated transaction gossip payloads, computes the digest of the payload and
 * converts the transaction to its trits representation.
 */
public class PreProcessStage implements Stage {

    private FIFOCache<Long, Hash> recentlySeenBytesCache;
    private IotaConfig config;

    /**
     * Creates a new {@link PreProcessStage}.
     *
     * @param recentlySeenBytesCache The cache to use for checking whether a transaction is known
     */
    public PreProcessStage(FIFOCache<Long, Hash> recentlySeenBytesCache, IotaConfig config) {
        this.recentlySeenBytesCache = recentlySeenBytesCache;
        this.config = config;
    }

    /**
     * Extracts the transaction gossip payload, expands it, computes the digest and then creates a new
     * {@link ProcessingContext} to the appropriate stage. If the transaction is not known, the transaction payload is
     * also converted to its trits representation.
     * 
     * @param ctx the pre process stage {@link ProcessingContext}
     * @return a {@link ProcessingContext} which either redirects to the {@link ReplyStage} or {@link HashingStage}
     *         depending on whether the transaction is known
     */
    public ProcessingContext process(ProcessingContext ctx) {
        PreProcessPayload payload = (PreProcessPayload) ctx.getPayload();
        ByteBuffer packetData = payload.getData();
        byte[] data = packetData.array();

        // allocate buffers for tx payload and requested tx hash
        byte[] txDataBytes = new byte[Transaction.SIZE];
        byte[] reqHashBytes = new byte[Protocol.GOSSIP_REQUESTED_TX_HASH_BYTES_LENGTH];

        // expand received tx data
        Protocol.expandTx(data, txDataBytes);

        // copy requested hash
        Protocol.extractRequestedTxHash(data, reqHashBytes);

        if (config.getPreProcessSleepMillisec() > 0) {
            try {
                Thread.sleep(config.getPreProcessSleepMillisec());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // increment all txs count
        payload.getOriginNeighbor().getMetrics().incrAllTransactionsCount();

        // compute digest of tx bytes data
        long txDigest = NeighborRouter.getTxCacheDigest(txDataBytes);

        Hash receivedTxHash = recentlySeenBytesCache.get(txDigest);
        Hash requestedHash = HashFactory.TRANSACTION.create(reqHashBytes, 0,
                Protocol.GOSSIP_REQUESTED_TX_HASH_BYTES_LENGTH);

        // received tx is known, therefore we can submit to the reply stage directly.
        if (receivedTxHash != null) {
            // reply with a random tip by setting the request hash to the null hash
            requestedHash = requestedHash.equals(receivedTxHash) ? Hash.NULL_HASH : requestedHash;
            ctx.setNextStage(TransactionProcessingPipeline.Stage.REPLY);
            ctx.setPayload(new ReplyPayload(payload.getOriginNeighbor(), requestedHash));
            return ctx;
        }

        // convert tx byte data into trits representation once
        byte[] txTrits = new byte[TransactionViewModel.TRINARY_SIZE];
        Converter.getTrits(txDataBytes, txTrits);

        // submit to hashing stage.
        ctx.setNextStage(TransactionProcessingPipeline.Stage.HASHING);
        HashingPayload hashingStagePayload = new HashingPayload(payload.getOriginNeighbor(), txTrits, txDigest,
                requestedHash);
        ctx.setPayload(hashingStagePayload);
        return ctx;
    }
}
