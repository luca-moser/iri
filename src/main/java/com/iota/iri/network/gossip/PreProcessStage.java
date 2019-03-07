package com.iota.iri.network.gossip;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.persistables.Transaction;
import com.iota.iri.utils.Converter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PreProcessStage implements Runnable {

    public final static int REQ_HASH_SIZE = 46;
    private static final Logger log = LoggerFactory.getLogger(Gossip.class);

    private ReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private HashMap<Long, Hash> recenltySeenCache = new HashMap<>();

    private BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue;

    // out
    private BlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> txValidationStageQueue;
    private BlockingQueue<Pair<Peer, Hash>> replyStageQueue;

    public PreProcessStage(
            BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue,
            BlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> txValidationStageQueue,
            BlockingQueue<Pair<Peer, Hash>> replyStageQueue) {
        this.preProcessStageQueue = preProcessStageQueue;
        this.txValidationStageQueue = txValidationStageQueue;
        this.replyStageQueue = replyStageQueue;
    }

    @Override
    public void run() {
        log.info("pre-process stage ready");

        while (!Gossip.SHUTDOWN.get()) {
            try {
                Pair<Peer, ByteBuffer> tuple = preProcessStageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (tuple == null) {
                    continue;
                }
                Peer peer = tuple.getLeft();
                ByteBuffer packetData = tuple.getRight();

                byte[] data = packetData.array();

                // extract tx data and request hash
                byte[] txDataBytes = new byte[Transaction.SIZE];
                byte[] reqHashBytes = new byte[REQ_HASH_SIZE];
                System.arraycopy(data, 0, txDataBytes, 0, Transaction.SIZE);
                System.arraycopy(data, Transaction.SIZE, reqHashBytes, 0, REQ_HASH_SIZE);

                // compute digest of tx bytes data
                long txDigest = Gossip.getTxCacheDigest(txDataBytes);

                cacheLock.readLock().lock();
                Hash receivedTxHash = recenltySeenCache.get(txDigest);
                cacheLock.readLock().unlock();

                Hash requestedHash = HashFactory.TRANSACTION.create(reqHashBytes, 0, REQ_HASH_SIZE);

                // received tx is known, therefore we can submit to the reply stage directly.
                if (receivedTxHash != null) {
                    // reply with a random tip by setting the request hash to the null hash
                    requestedHash = requestedHash.equals(receivedTxHash) ? Hash.NULL_HASH : requestedHash;
                    replyStageQueue.put(new ImmutablePair<>(peer, requestedHash));
                    continue;
                }

                // convert tx byte data into trits representation once
                byte[] txTrits = new byte[TransactionViewModel.TRINARY_SIZE];
                Converter.getTrits(txDataBytes, txTrits);

                // submit to validation stage.
                // note that the validation stage takes care of submitting a payload to the reply stage.
                txValidationStageQueue.put(new ImmutablePair<>(peer, new ImmutableTriple<>(txTrits, txDigest, requestedHash)));
            } catch (InterruptedException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        log.info("pre-process stage stopped");
    }

    public void cacheHash(long dataDigest, Hash hash) {
        cacheLock.writeLock().lock();
        try {
            recenltySeenCache.put(dataDigest, hash);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
}
