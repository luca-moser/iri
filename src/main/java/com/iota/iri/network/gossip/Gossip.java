package com.iota.iri.network.gossip;

import com.iota.iri.TransactionValidator;
import com.iota.iri.conf.NodeConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Gossip implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Gossip.class);

    public final static AtomicBoolean SHUTDOWN = new AtomicBoolean(false);

    private NodeConfig config;
    private TransactionRequester txRequester;
    private static final SecureRandom rnd = new SecureRandom();

    // stages of the gossip protocol
    private PreProcessStage preProcessStage;
    private ReceivedStage receiveStage;
    private TxValidationStage txValidationStage;
    private ReplyStage replyStage;
    private BroadcastStage broadcastStage;

    private ArrayBlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue = new ArrayBlockingQueue<>(128);
    private ArrayBlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> txValidationStageQueue = new ArrayBlockingQueue<>(128);
    private ArrayBlockingQueue<Pair<Peer, TransactionViewModel>> receivedStageQueue = new ArrayBlockingQueue<>(128);
    private ArrayBlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue = new ArrayBlockingQueue<>(128);
    private ArrayBlockingQueue<Pair<Peer, Hash>> replyStageQueue = new ArrayBlockingQueue<>(128);

    private TipRequester tipRequester;
    private Executor stagesThreadPool = Executors.newFixedThreadPool(6);

    public Gossip(
            NodeConfig config, TransactionValidator txValidator, Tangle tangle,
            SnapshotProvider snapshotProvider, TransactionRequester txRequester,
            TipsViewModel tipsViewModel, LatestMilestoneTracker latestMilestoneTracker
    ) {
        this.config = config;
        this.txRequester = txRequester;
        this.replyStage = new ReplyStage(replyStageQueue, this, config, tangle, tipsViewModel, latestMilestoneTracker, preProcessStage, txRequester);
        this.broadcastStage = new BroadcastStage(broadcastStageQueue, this);
        this.preProcessStage = new PreProcessStage(preProcessStageQueue, txValidationStageQueue, replyStageQueue);
        this.txValidationStage = new TxValidationStage(txValidationStageQueue, receivedStageQueue, replyStageQueue, preProcessStage, txValidator);
        this.receiveStage = new ReceivedStage(receivedStageQueue, broadcastStageQueue, tangle, txValidator, snapshotProvider);
        this.tipRequester = new TipRequester(this, tangle, latestMilestoneTracker, txRequester);
    }

    private HashMap<String, Peer> peers = new HashMap<>();

    public HashMap<String, Peer> getPeers() {
        return peers;
    }

    public void sendPacket(Peer peer, TransactionViewModel tvm) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(Peer.PACKET_SIZE);
        Hash hash = txRequester.transactionToRequest(rnd.nextDouble() < config.getpSelectMilestoneChild());
        buf.put(tvm.getBytes());

        if (hash != null) {
            buf.put(hash.bytes(), 0, PreProcessStage.REQ_HASH_SIZE);
        } else {
            buf.put(tvm.getHash().bytes(), 0, PreProcessStage.REQ_HASH_SIZE);
        }

        buf.flip();
        peer.send(buf);
    }

    @Override
    public void run() {

        // build tx pipeline
        stagesThreadPool.execute(preProcessStage);
        stagesThreadPool.execute(txValidationStage);
        stagesThreadPool.execute(replyStage);
        stagesThreadPool.execute(receiveStage);
        stagesThreadPool.execute(broadcastStage);
        stagesThreadPool.execute(tipRequester);

        // run selector loop
        try (Selector selector = Selector.open()) {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            serverSocketChannel.socket().bind(new InetSocketAddress("0.0.0.0", config.getTcpReceiverPort()));
            log.info("bound server TCP socket");

            // connect to all defined peers
            List<InetSocketAddress> peerAddrs = config.getNeighbors().stream().distinct()
                    .filter(s -> !s.isEmpty())
                    .map(Gossip::uri).map(Optional::get)
                    .filter(this::isURIValid)
                    .map(uri -> new InetSocketAddress(uri.getHost(), uri.getPort()))
                    .collect(Collectors.toList());

            for (InetSocketAddress neighborAddr : peerAddrs) {
                SocketChannel peerConn = SocketChannel.open();
                peerConn.configureBlocking(false);
                peerConn.connect(neighborAddr);
                String peerID = peerID(peerConn);
                Peer peer = new Peer(selector, peerConn, peerID, preProcessStageQueue);
                peerConn.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE | SelectionKey.OP_READ, peer);
            }

            while (!SHUTDOWN.get()) {
                int selected = selector.select();

                for (SelectionKey key : selector.selectedKeys()) {

                    if (key.isAcceptable()) {
                        // new connection from peer
                        ServerSocketChannel srvSocket = (ServerSocketChannel) key.channel();
                        SocketChannel newPeerConn = srvSocket.accept();
                        if (newPeerConn == null) {
                            continue;
                        }
                        String peerID = peerID(newPeerConn);
                        if (peers.containsKey(peerID)) {
                            log.info("dropping new connection from {} as peer is already connected", peerID);
                            newPeerConn.close();
                            continue;
                        }
                        newPeerConn.configureBlocking(false);
                        Peer newPeer = new Peer(selector, newPeerConn, peerID, preProcessStageQueue);
                        log.info("peer connected {}", newPeer.getId());
                        newPeerConn.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, newPeer);
                        peers.put(peerID, newPeer);
                        continue;
                    }

                    SocketChannel channel = (SocketChannel) key.channel();
                    Peer peer = (Peer) key.attachment();

                    if (key.isConnectable()) {
                        log.info("finishing connection to {}", peer.getId());
                        try {
                            if (peers.containsKey(peer.getId())) {
                                log.info("peer already connected {}", peer.getId());
                                key.cancel();
                                continue;
                            }
                            if (channel.finishConnect()) {
                                log.info("peer connected {}", peer.getId());
                                peers.put(peer.getId(), peer);
                                continue;
                            }
                        } catch (ConnectException ex) {
                            log.info("couldn't build connection to {}", peer.getId());
                            removeDisconnectedPeer(channel, peer.getId());
                        }
                        continue;
                    }

                    if (key.isReadable()) {
                        //log.info("reading from peer {}", peer.getId());
                        if (peer.read() == -1) {
                            removeDisconnectedPeer(channel, peer.getId());
                            continue;
                        }
                    }

                    if (key.isWritable()) {
                        //log.info("writing to peer {}", peer.getId());
                        switch (peer.write()) {
                            case 0:
                                // nothing was written, probably because no message was available to send.
                                // lets unregister this channel from writing events until at least
                                // one message is back available for sending.
                                key.interestOps(SelectionKey.OP_READ);
                                break;
                            case -1:
                                removeDisconnectedPeer(channel, peer.getId());
                                break;
                        }
                    }
                }
                selector.selectedKeys().clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("gossip protocol stopped");
        }
    }

    private static LongHashFunction txCacheDigestHashFunc = LongHashFunction.xx();;

    public static long getTxCacheDigest(byte[] receivedData) throws NoSuchAlgorithmException {
        return txCacheDigestHashFunc.hashBytes(receivedData);
    }

    private void removeDisconnectedPeer(SocketChannel peerChannel, String id) throws IOException {
        log.info("removing peer {} from connected peers", id);
        peerChannel.close();
        peers.remove(id);
    }

    private String peerID(SocketChannel peerChannel) throws IOException {
        SocketAddress socketAddr = peerChannel.getRemoteAddress();
        log.warn("socket address peerID {}", socketAddr);
        return socketAddr.toString();
    }

    public static Optional<URI> uri(final String uri) {
        try {
            return Optional.of(new URI(uri));
        } catch (URISyntaxException e) {
            log.error("URI {} raised URI Syntax Exception", uri);
        }
        return Optional.empty();
    }

    public boolean isURIValid(final URI uri) {
        if (uri == null) {
            log.error("Cannot read URI schema, please check neighbor config!");
            return false;
        }

        if (uri.getScheme().equals("tcp") || uri.getScheme().equals("udp")) {
            if ((new InetSocketAddress(uri.getHost(), uri.getPort()).getAddress() != null)) {
                return true;
            }
        }
        log.error("'{}' is not a valid URI schema or resolvable address.", uri);
        return false;
    }


    public ArrayBlockingQueue<Pair<Peer, TransactionViewModel>> getReceivedStageQueue() {
        return receivedStageQueue;
    }

    public ArrayBlockingQueue<Pair<Peer, TransactionViewModel>> getBroadcastStageQueue() {
        return broadcastStageQueue;
    }

    public ArrayBlockingQueue<Pair<Peer, Hash>> getReplyStageQueue() {
        return replyStageQueue;
    }

    public ArrayBlockingQueue<Pair<Peer, Triple<byte[], Long, Hash>>> getTxValidationStageQueue() {
        return txValidationStageQueue;
    }

    public void shutdown() {
        SHUTDOWN.set(true);
    }
}
