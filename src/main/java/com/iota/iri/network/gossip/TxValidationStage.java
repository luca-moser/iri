package com.iota.iri.network.gossip;

import com.iota.iri.TransactionValidator;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.Curl;
import com.iota.iri.crypto.bct.BatchHasher;
import com.iota.iri.crypto.bct.HashReq;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.TransactionHash;
import com.iota.iri.utils.Converter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.iota.iri.model.Hash.SIZE_IN_TRITS;

public class TxValidationStage implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TxValidationStage.class);

    private PreProcessStage preProcessStage;
    private BatchHasher hasher = new BatchHasher(Curl.HASH_LENGTH, 81);

    private BlockingQueue<Triple<Peer, Triple<byte[], Long, Hash>, Boolean>> txValidationStageQueue;

    // out
    private BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> receivedStageQueue;
    private BlockingQueue<Pair<Peer, Hash>> replyStageQueue;
    private TransactionValidator txValidator;

    public TxValidationStage(
            BlockingQueue<Triple<Peer, Triple<byte[], Long, Hash>, Boolean>> txValidationStageQueue,
            BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> receivedStageQueue,
            BlockingQueue<Pair<Peer, Hash>> replyStageQueue,
            PreProcessStage preProcessStage, TransactionValidator txValidator) {
        this.txValidationStageQueue = txValidationStageQueue;
        this.replyStageQueue = replyStageQueue;
        this.receivedStageQueue = receivedStageQueue;
        this.preProcessStage = preProcessStage;
        this.txValidator = txValidator;
    }

    @Override
    public void run() {
        Thread hasherThread = new Thread(() -> {
            try {
                hasher.runDispatcher();
            } catch (InterruptedException e) {
                // TODO: log
            }
        });
        hasherThread.start();

        log.info("tx-validation stage ready");
        while (!Gossip.SHUTDOWN.get()) {
            try {
                Triple<Peer, Triple<byte[], Long, Hash>, Boolean> triple = txValidationStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (triple == null) {
                    continue;
                }
                Peer peer = triple.getLeft();
                Triple<byte[], Long, Hash> data = triple.getMiddle();
                boolean bypass = triple.getRight();
                final byte[] txTrits = data.getLeft();
                final Long txDigest = data.getMiddle();
                final Hash requestedHash = data.getRight();

                // submit a new hash request
                HashReq hashReq = new HashReq();
                hashReq.input = txTrits;
                hashReq.callback = hashTrits -> {
                    TransactionHash txHash = (TransactionHash) HashFactory.TRANSACTION.create(hashTrits, 0, SIZE_IN_TRITS);
                    TransactionViewModel tvm = new TransactionViewModel(txTrits, txHash);
                    try {
                        txValidator.runValidation(tvm, txValidator.getMinWeightMagnitude());
                    } catch (Exception ex) {
                        log.error("invalid tx received");
                        return;
                    }

                    // cache the tx hash under the tx payload digest
                    if (txDigest != null && txDigest != 0) {
                        preProcessStage.cacheHash(txDigest, txHash);
                    }

                    try {
                        // the requested hash might be null because the input came from a broadcastTransactions command
                        if (peer != null && requestedHash != null) {
                            Hash txToRequest = requestedHash.equals(txHash) ? Hash.NULL_HASH : requestedHash;
                            replyStageQueue.put(new ImmutablePair<>(peer, txToRequest));
                        }
                        receivedStageQueue.put(new ImmutableTriple<>(peer, tvm, bypass));
                    } catch (InterruptedException e) {
                        log.error("unable to pass data to received stage");
                    }
                };
                hasher.hash(hashReq);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("tx-validation stage stopped");
    }
}
