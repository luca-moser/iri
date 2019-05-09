package com.iota.iri.service.warpsync;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.persistables.Transaction;
import com.iota.iri.network.NeighborRouter;
import com.iota.iri.network.neighbor.Neighbor;
import com.iota.iri.network.pipeline.TransactionProcessingPipeline;
import com.iota.iri.network.protocol.Protocol;
import com.iota.iri.network.protocol.ProtocolMessage;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Converter;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;
import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarpSyncer {

    public final static AtomicBoolean IS_WARP_SYNCING = new AtomicBoolean(false);
    private final static int MESSAGE_RECEIVE_TIMEOUT_SEC = 5;
    private final static int BYTES_TO_MEGABYTES = 1048576;

    private static final Logger log = LoggerFactory.getLogger(WarpSyncer.class);
    private final ThreadIdentifier warpSyncerThreadIdentifier = new ThreadIdentifier("Warp Syncer");
    private Thread warpSyncerThread;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // external
    private IotaConfig config;
    private Tangle tangle;
    private SnapshotProvider snapshotProvider;
    private LatestMilestoneTracker latestMilestoneTracker;
    private NeighborRouter neighborRouter;
    private TransactionProcessingPipeline pipeline;

    // current sender specific data
    private Neighbor currentSender;
    private Neighbor previousSender;

    // current receiver specific data
    private ReentrantLock currentReceiverLock = new ReentrantLock();
    private Neighbor currentReceiver;
    private int currentReceiverMilestoneTargetIndex;
    private List<Hash> transactionsToSend;

    // cancellation/ready signals
    private AtomicBoolean processCancelled = new AtomicBoolean(false);
    private BlockingQueue<Pair<Neighbor, ByteBuffer>> readySignal = new ArrayBlockingQueue<>(1);

    // current step
    private long amountOfTransactionsToReceive = 0;
    private BlockingQueue<ByteBuffer> receive = new ArrayBlockingQueue<>(100);

    // mode
    private final Object modeLock = new Object();
    private Mode mode = Mode.INIT;

    public void init(IotaConfig config, Tangle tangle, SnapshotProvider snapshotProvider,
            LatestMilestoneTracker latestMilestoneTracker, NeighborRouter neighborRouter,
            TransactionProcessingPipeline pipeline) {
        this.config = config;
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.neighborRouter = neighborRouter;
        this.pipeline = pipeline;
    }

    private enum Mode {
        INIT, RECEIVING, SENDING
    }

    public void start() {
        warpSyncerThread = ThreadUtils.spawnThread(this::run, warpSyncerThreadIdentifier);
    }

    public void shutdown() {
        shutdown.set(true);
        ThreadUtils.stopThread(warpSyncerThreadIdentifier);
    }

    private void run() {
        log.info("started warp syncer thread");
        while (!shutdown.get()) {

            int currentMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
            int latestMilestoneIndex = latestMilestoneTracker.getLatestMilestoneIndex();

            // check whether we got a warp sync request and the necessary data to fulfill the request
            try {
                if (shouldSend(currentMilestoneIndex, latestMilestoneIndex)) {
                    try {
                        send();
                    } catch (InterruptedException e) {
                        continue;
                    }
                }
            } finally {
                resetState();
            }

            try {
                // check whether we should request a warp sync ourselves.
                // synchronize as long as we aren't synced up and there isn't any problem
                // while receiving transactions
                while (shouldSynchronize(currentMilestoneIndex, latestMilestoneIndex)) {
                    log.info("threshold reached, requesting warp sync from neighbors...");
                    try {
                        if ((currentSender = sendWarpSyncRequest()) == null) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        break;
                    }

                    if (!synchronize()) {
                        break;
                    }

                    currentMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
                    latestMilestoneIndex = latestMilestoneTracker.getLatestMilestoneIndex();
                }
            } finally {
                resetState();
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
            }
        }
        log.info("warp sync stopped");
    }

    private void send() throws InterruptedException {
        synchronized (modeLock) {
            mode = Mode.SENDING;
        }

        // send an ok packet to the requesting neighbor
        log.info("will acknowledge the warp sync request from {}", currentReceiver.getHostAddressAndPort());
        currentReceiver.send(
                createWarpSyncOkPacket(snapshotProvider.getLatestSnapshot().getIndex(), transactionsToSend.size()));

        // await start message
        // TODO: handle interrupt/cancellation signal immediately
        Pair<Neighbor, ByteBuffer> startMsg = readySignal.poll(MESSAGE_RECEIVE_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (startMsg == null) {
            // if the receiver didn't cancel explicitly, we tell it that we now cancel the warp syncing
            if (!processCancelled.get()) {
                log.info("{} didn't respond in time to our acknowledgement, cancelling warp sync...",
                        currentReceiver.getHostAddressAndPort());
                currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.NOT_REPLIED_IN_TIME));
            }
            // the else case is handled within cancelFrom()
            return;
        }

        // worst case scenario
        long approxSizeOfDataToSend = (transactionsToSend.size() * Transaction.SIZE) / BYTES_TO_MEGABYTES;
        log.info("got warp sync start signal from {}, sending off {} transactions (~{} MB) ",
                currentReceiver.getHostAddressAndPort(), transactionsToSend.size(), approxSizeOfDataToSend);

        // stream transactions from the database directly to the given receiver
        long bytesSent = 0;
        int sentOff = 0;
        long start = System.currentTimeMillis();
        for (Hash hash : transactionsToSend) {
            TransactionViewModel tvm;
            try {
                tvm = TransactionViewModel.fromHash(tangle, hash);
            } catch (Exception e) {
                // can happen because of pruning
                currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
                continue;
            }
            if (processCancelled.get()) {
                // log handled within cancelFrom()
                return;
            }
            // same applies here, the transaction bytes could be pruned away in the meantime
            byte[] txBytes = tvm.getBytes();
            if (txBytes == null) {
                // can happen because of pruning
                currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
                continue;
            }
            ByteBuffer buf = createWarpSyncTxPacket(Protocol.truncateTx(txBytes));
            bytesSent += buf.capacity();
            currentReceiver.send(buf);
            sentOff++;
        }

        log.info("sent off {} transactions (~{} MB) to {}, took {} seconds", sentOff, bytesSent / BYTES_TO_MEGABYTES,
                currentReceiver.getHostAddressAndPort(), System.currentTimeMillis() - start / 1000);
        Pair<Neighbor, ByteBuffer> thxMsg = readySignal.poll(MESSAGE_RECEIVE_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (thxMsg == null) {
            log.info("didn't get acknowledgement message from {} in time but warp sync was successful",
                    currentReceiver.getHostAddressAndPort());
        }
    }

    private boolean shouldSend(int currentMilestoneIndex, int latestMilestoneIndex) {
        if (currentReceiver == null) {
            return false;
        }

        log.info("got a warp sync request from {} to milestone {}", currentReceiver.getHostAddressAndPort(),
                currentReceiverMilestoneTargetIndex);
        int delta = latestMilestoneIndex - currentMilestoneIndex;

        // won't sync if we aren't fully synced ourselves
        if (delta >= config.getWarpSyncDeltaThreshold()) {
            log.info("will cancel request as we are not synced ourselves");
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.NOT_SYNCED));
            return false;
        }

        // can't sync up into the future
        if (currentReceiverMilestoneTargetIndex > currentMilestoneIndex) {
            log.info("will cancel request as requested target milestone {} is in the future",
                    currentReceiverMilestoneTargetIndex);
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.MILESTONE_TARGET_TOO_NEW));
            return false;
        }

        // check whether we actually have the data for the requested milestone
        MilestoneViewModel milestoneViewModel;
        try {
            milestoneViewModel = MilestoneViewModel.get(tangle, currentReceiverMilestoneTargetIndex);
            if (milestoneViewModel == null) {
                log.info("will cancel request as requested target milestone {} is not in our database",
                        currentReceiverMilestoneTargetIndex);
                currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
                return false;
            }
        } catch (Exception e) {
            log.info("will cancel request as requested target milestone {} couldn't be loaded from the database",
                    currentReceiverMilestoneTargetIndex);
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
            return false;
        }

        // check whether the milestone is actually solid
        TransactionViewModel msTx;
        try {
            msTx = TransactionViewModel.fromHash(tangle, milestoneViewModel.getHash());
        } catch (Exception e) {
            log.info("will cancel request as requested target milestone {} couldn't be loaded from the database",
                    currentReceiverMilestoneTargetIndex);
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
            return false;
        }
        if (msTx == null || !msTx.isSolid()) {
            if (msTx == null) {
                log.info("will cancel request as requested target milestone {} couldn't be loaded from the database",
                        currentReceiverMilestoneTargetIndex);
            } else {
                log.info("will cancel request as requested target milestone {} is not solid",
                        currentReceiverMilestoneTargetIndex);
            }
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
            return false;
        }

        // load up all hashes of transactions which got confirmed by the requested target milestone
        MilestoneViewModel finalMilestoneViewModel = milestoneViewModel;
        transactionsToSend = new ArrayList<>(1000);
        try {
            DAGHelper.get(tangle).traverseApprovees(milestoneViewModel.getHash(),
                    // we use >= as later milestones may confirm a given transaction again
                    approvedTransaction -> approvedTransaction.snapshotIndex() >= finalMilestoneViewModel.index(),
                    approvedTransaction -> transactionsToSend.add(approvedTransaction.getHash()));
        } catch (TraversalException e) {
            log.info("will cancel request as a transaction which should be sent isn't available in the database");
            currentReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.INSUFFICIENT_DATA));
            return false;
        }

        return true;
    }

    private void resetState() {
        synchronized (modeLock) {
            mode = Mode.INIT;
            processCancelled.set(false);
            readySignal.clear();
            transactionsToSend.clear();
            try {
                currentReceiverLock.lock();
                currentReceiver = null;
                currentReceiverMilestoneTargetIndex = 0;
            } finally {
                currentReceiverLock.unlock();
            }
        }
    }

    private boolean shouldSynchronize(int currentMilestoneIndex, int latestMilestoneIndex) {
        int delta = latestMilestoneIndex - currentMilestoneIndex;
        log.info("current LSM {}, latest LM {}, delta {}, threshold {}", currentMilestoneIndex, latestMilestoneIndex,
                delta, config.getWarpSyncDeltaThreshold());
        return delta >= config.getWarpSyncDeltaThreshold();
    }

    public boolean synchronize() {

        // flag for other components to behave differently while warp sync is ongoing
        IS_WARP_SYNCING.set(true);
        try {
            // receive data from the given currentSender
            long shouldReceive = amountOfTransactionsToReceive;
            long start = System.currentTimeMillis();
            for (; amountOfTransactionsToReceive != 0; amountOfTransactionsToReceive--) {
                ByteBuffer msg = receive.poll(MESSAGE_RECEIVE_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (msg == null) {
                    if (processCancelled.get()) {
                        log.info("aborting warp sync, since {} cancelled", currentSender.getHostAddressAndPort());
                    } else {
                        log.info("aborting warp syncing, as {} didn't send anything within {} seconds",
                                currentSender.getHostAddressAndPort(), MESSAGE_RECEIVE_TIMEOUT_SEC);
                        currentSender.send(createWarpCancelPacket(WarpSyncCancellationReason.TX_RECEIVE_TIMEOUT));
                    }
                    return false;
                }

                // log progress
                long now = System.currentTimeMillis();
                if (now - start > TimeUnit.SECONDS.toMillis(5)) {
                    start = now;
                    long received = shouldReceive - amountOfTransactionsToReceive;
                    double percentageDone = Math.floor((received / shouldReceive) * 100);
                    log.info("received {}/{} ({}%)", received, shouldReceive, percentageDone);
                }

                byte[] expandedTxBytes = new byte[Transaction.SIZE];
                byte[] txTrits = new byte[TransactionViewModel.TRINARY_SIZE];
                Protocol.expandTx(msg.array(), expandedTxBytes);
                Converter.getTrits(expandedTxBytes, txTrits);
                pipeline.process(txTrits);
            }

            // remember the current used sender
            previousSender = currentSender;
            currentSender = null;
        } catch (InterruptedException e) {
            log.info("warp syncer got interrupted, while receiving txs...");
            currentSender.send(createWarpCancelPacket(WarpSyncCancellationReason.INTERRUPTED));
            return false;
        } finally {
            synchronized (modeLock) {
                mode = Mode.INIT;
            }
            IS_WARP_SYNCING.set(false);
        }

        return true;
    }

    private Neighbor sendWarpSyncRequest() throws InterruptedException {
        Map<String, Neighbor> neighbors = neighborRouter.getConnectedNeighbors();
        if (neighbors.size() == 0) {
            log.info("unable to warp sync since no neighbors are connected");
            return null;
        }

        // reset the previous sender if only one neighbor is connected
        // as otherwise we wouldn't send the request packet to it
        if (neighbors.size() == 1) {
            previousSender = null;
        }

        // send each neighbor a packet
        int currentMilestoneIndex = snapshotProvider.getInitialSnapshot().getIndex();
        log.info("requesting warp sync from {} neighbors...", neighbors.size());
        for (Neighbor neighbor : neighbors.values()) {
            // skip previously used sender
            if (neighbor == previousSender) {
                continue;
            }
            neighbor.send(createWarpRequestPacket(currentMilestoneIndex + 1));
        }

        Pair<Neighbor, ByteBuffer> firstReady = readySignal.poll(MESSAGE_RECEIVE_TIMEOUT_SEC, TimeUnit.SECONDS);

        // send a cancel message to each neighbor which didn't reply in time
        for (Neighbor neighbor : neighbors.values()) {
            if (neighbor == firstReady) {
                continue;
            }
            neighbor.send(createWarpCancelPacket(WarpSyncCancellationReason.NOT_REPLIED_IN_TIME));
        }

        // no neighbor replied in time
        if (firstReady == null) {
            previousSender = null;
            log.info("no neighbor sent us an acknowledgement in time, aborting...");
            return null;
        }

        // signal the selected neighbor to start sending
        ByteBuffer okMsg = firstReady.getRight();
        int latestKnownMilestoneIndexBySender = okMsg.getInt();
        amountOfTransactionsToReceive = okMsg.getLong();
        log.info(
                "neighbor {} acknowledged our warp sync request, its LM index is {}. will receive {} transactions (~{} MB), hold on tight...",
                firstReady.getLeft().getHostAddressAndPort(), latestKnownMilestoneIndexBySender,
                amountOfTransactionsToReceive, (amountOfTransactionsToReceive * Transaction.SIZE) / BYTES_TO_MEGABYTES);

        // clear the receive queue
        receive.clear();

        // we are now receiving
        synchronized (modeLock) {
            mode = Mode.RECEIVING;
        }

        // send signal to start sending transactions
        firstReady.getLeft().send(createWarpSyncStartPacket());
        return firstReady.getLeft();
    }

    public void txFrom(Neighbor neighbor, ByteBuffer txsMsg) {
        // this is only here as a safety measure, if it actually occurs in practice
        // then the given neighbor must be using a modified IRI version
        if (neighbor != currentSender) {
            return;
        }
        try {
            receive.put(txsMsg);
        } catch (InterruptedException e) {
            log.warn("got interrupted while adding a transaction message into the receive queue via {}",
                    neighbor.getHostAddressAndPort());
        }
    }

    public void okFrom(Neighbor neighbor, ByteBuffer okMsg) {
        synchronized (modeLock) {
            if (mode == Mode.RECEIVING) {
                log.info("neighbor {} acknowledged our warp sync request but was too slow to respond",
                        neighbor.getHostAddressAndPort());
                return;
            }
        }
        readySignal.offer(new ImmutablePair<>(neighbor, okMsg));
    }

    public void startFrom(Neighbor neighbor, ByteBuffer startMsg) {
        if (neighbor != currentReceiver) {
            return;
        }
        synchronized (modeLock) {
            if (mode != Mode.SENDING) {
                return;
            }
        }
        readySignal.offer(new ImmutablePair<>(neighbor, startMsg));
    }

    public void cancelFrom(Neighbor neighbor, ByteBuffer msg) {
        WarpSyncCancellationReason reason = WarpSyncCancellationReason.fromValue(msg.get());
        switch (mode) {
            case INIT:
            case RECEIVING:
                log.info("neighbor {} cancelled the warp sync with reason {}", neighbor.getHostAddressAndPort(),
                        reason);
                if (neighbor == currentSender) {
                    processCancelled.set(true);
                    // short-circuit polling on the receive queue
                    warpSyncerThread.interrupt();
                }
                break;
            case SENDING:
                if (neighbor == currentReceiver) {
                    log.info("neighbor {} cancelled the warp sync with reason {}", neighbor.getHostAddressAndPort(),
                            reason);
                    processCancelled.set(true);
                }
                break;
        }
    }

    public void syncRequestFrom(Neighbor potentialReceiver, ByteBuffer msg) {
        try {
            currentReceiverLock.lock();
            if (currentReceiver != null) {
                potentialReceiver.send(createWarpCancelPacket(WarpSyncCancellationReason.REQUEST_SLOT_FILLED));
                return;
            }
            currentReceiver = potentialReceiver;
            currentReceiverMilestoneTargetIndex = msg.getInt();
        } finally {
            currentReceiverLock.unlock();
        }
    }

    public ByteBuffer createWarpRequestPacket(int targetMilestoneIndex) {
        ByteBuffer buf = ByteBuffer
                .allocate(ProtocolMessage.HEADER.getMaxLength() + ProtocolMessage.WARP_SYNC_REQUEST.getMaxLength());
        Protocol.addProtocolHeader(buf, ProtocolMessage.WARP_SYNC_REQUEST);
        buf.putInt(targetMilestoneIndex);
        buf.flip();
        return buf;
    }

    public ByteBuffer createWarpCancelPacket(WarpSyncCancellationReason reason) {
        ByteBuffer buf = ByteBuffer
                .allocate(ProtocolMessage.HEADER.getMaxLength() + ProtocolMessage.WARP_SYNC_CANCEL.getMaxLength());
        Protocol.addProtocolHeader(buf, ProtocolMessage.WARP_SYNC_CANCEL);
        buf.put(reason.getID());
        buf.flip();
        return buf;
    }

    public ByteBuffer createWarpSyncOkPacket(int latestKnownMilestoneIndex, long amountOfTxsToSend) {
        ByteBuffer buf = ByteBuffer
                .allocate(ProtocolMessage.HEADER.getMaxLength() + ProtocolMessage.WARP_SYNC_OK.getMaxLength());
        Protocol.addProtocolHeader(buf, ProtocolMessage.WARP_SYNC_OK);
        buf.putInt(latestKnownMilestoneIndex);
        buf.putLong(amountOfTxsToSend);
        buf.flip();
        return buf;
    }

    public ByteBuffer createWarpSyncTxPacket(byte[] tx) {
        ByteBuffer buf = ByteBuffer.allocate(ProtocolMessage.HEADER.getMaxLength() + tx.length);
        Protocol.addProtocolHeader(buf, ProtocolMessage.WARP_SYNC_TX, (short) tx.length);
        buf.put(tx);
        buf.flip();
        return buf;
    }

    public ByteBuffer createWarpSyncStartPacket() {
        ByteBuffer buf = ByteBuffer
                .allocate(ProtocolMessage.HEADER.getMaxLength() + ProtocolMessage.WARP_SYNC_START.getMaxLength());
        Protocol.addProtocolHeader(buf, ProtocolMessage.WARP_SYNC_START);
        buf.put((byte) 1);
        buf.flip();
        return buf;
    }

}
