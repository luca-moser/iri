package com.iota.iri.network.gossip;

import com.iota.iri.TransactionValidator;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.Curl;
import com.iota.iri.crypto.bct.BatchHasher;
import com.iota.iri.crypto.bct.HashReq;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.TransactionHash;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

    private BlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> txValidationStageQueue;

    // out
    private BlockingQueue<Pair<Peer, TransactionViewModel>> receivedStageQueue;
    private BlockingQueue<Pair<Peer, Hash>> replyStageQueue;
    private TransactionValidator txValidator;

    public TxValidationStage(
            BlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> txValidationStageQueue,
            BlockingQueue<Pair<Peer, TransactionViewModel>> receivedStageQueue,
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
                Pair<Peer, Triple<byte[], Long, Hash>> tuple = txValidationStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                Peer peer = tuple.getLeft();
                Triple<byte[], Long, Hash> triple = tuple.getRight();
                byte[] txTrits = triple.getLeft();
                final Long txDigest = triple.getMiddle();
                final Hash requestedHash = triple.getRight();

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
                    if(txDigest != null && txDigest != 0){
                        preProcessStage.cacheHash(txDigest, txHash);
                    }

                    try {
                        // the requested hash might be null because the input came from a broadcastTransactions command
                        if (peer != null && requestedHash != null) {
                            Hash txToRequest = requestedHash.equals(txHash) ? Hash.NULL_HASH : requestedHash;
                            replyStageQueue.put(new ImmutablePair<>(peer, txToRequest));
                        }
                        receivedStageQueue.put(new ImmutablePair<>(peer, tvm));
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
