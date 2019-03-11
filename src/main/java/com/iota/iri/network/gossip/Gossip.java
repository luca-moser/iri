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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
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

    private BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Triple<Peer, Triple<byte[], Long, Hash>, Boolean>> txValidationStageQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> receivedStageQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Pair<Peer, TransactionViewModel>> broadcastStageQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Pair<Peer, Hash>> replyStageQueue = new LinkedBlockingQueue<>();

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

    private ConcurrentHashMap<String, Peer> peers = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, Peer> getPeers() {
        return peers;
    }

    public void sendPacket(Peer peer, TransactionViewModel tvm) throws Exception {
        sendPacket(peer, tvm, false);
    }

    public void sendPacket(Peer peer, TransactionViewModel tvm, boolean useHashOfTVM) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(Peer.PACKET_SIZE);
        buf.put(tvm.getBytes());

        Hash hash = null;
        if (!useHashOfTVM) {
            hash = txRequester.transactionToRequest(rnd.nextDouble() < config.getpSelectMilestoneChild());
        }

        if (hash != null) {
            buf.put(hash.bytes(), 0, PreProcessStage.REQ_HASH_SIZE);
        } else {
            buf.put(tvm.getHash().bytes(), 0, PreProcessStage.REQ_HASH_SIZE);
        }

        buf.flip();
        peer.send(buf);
    }

    private void initPeers(Selector selector) throws IOException {
        // parse URIs
        List<URI> peerURIs = config.getNeighbors().stream().distinct()
                .filter(s -> !s.isEmpty())
                .map(Gossip::uri).map(Optional::get)
                .filter(this::isURIValid)
                .collect(Collectors.toList());

        for (URI peerURI : peerURIs) {
            InetSocketAddress addr = new InetSocketAddress(peerURI.getHost(), peerURI.getPort());
            String peerID = peerID(addr);

            Peer peer;
            switch (peerURI.getScheme()) {
                case "tcp":
                    SocketChannel tcpChannel = SocketChannel.open();
                    tcpChannel.socket().setTcpNoDelay(true);
                    tcpChannel.socket().setSoLinger(true, 0);
                    tcpChannel.configureBlocking(false);
                    tcpChannel.connect(addr);
                    peer = new Peer(selector, tcpChannel, peerID, Peer.ConnectType.TCP, preProcessStageQueue);
                    tcpChannel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE | SelectionKey.OP_READ, peer);
                    // note that tcp peers are added to the peers map once they're actually fully connected
                    break;
                case "udp":
                    DatagramChannel udpChannel = DatagramChannel.open();
                    udpChannel.configureBlocking(false);
                    udpChannel.connect(addr);
                    peer = new Peer(selector, udpChannel, peerID, Peer.ConnectType.UDP, preProcessStageQueue);
                    udpChannel.register(selector, SelectionKey.OP_WRITE, peer);
                    peers.put(peerID, peer);
                    break;
                default:
                    throw new RuntimeException(String.format("invalid peer uri in config: %s", peerURI.toString()));
            }
        }
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
            ServerSocketChannel tcpSrvSocket = ServerSocketChannel.open();
            tcpSrvSocket.configureBlocking(false);
            tcpSrvSocket.register(selector, SelectionKey.OP_ACCEPT);
            InetSocketAddress tcpBindAddr = new InetSocketAddress("0.0.0.0", config.getTcpReceiverPort());
            tcpSrvSocket.socket().bind(tcpBindAddr);
            log.info("bound server TCP socket to {}", tcpBindAddr);

            DatagramChannel udpSrvSocket = DatagramChannel.open();
            InetSocketAddress udpBindAddr = new InetSocketAddress("0.0.0.0", config.getUdpReceiverPort());
            udpSrvSocket.configureBlocking(false);
            udpSrvSocket.register(selector, SelectionKey.OP_READ);
            udpSrvSocket.bind(udpBindAddr);
            log.info("bound server UDP socket to {}", udpBindAddr);

            // build up connections to peers
            initPeers(selector);

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
                        Peer newPeer = new Peer(selector, newPeerConn, peerID, Peer.ConnectType.TCP, preProcessStageQueue);
                        log.info("peer connected {}", newPeer.getId());
                        newPeerConn.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, newPeer);
                        peers.put(peerID, newPeer);
                        // if all peers are connected we are no longer interested in any incoming connections
                        // (as long as no peer dropped the connection)
                        if (config.getNeighbors().size() == peers.size()) {
                            key.interestOps(0);
                        }
                        continue;
                    }

                    SelectableChannel selectableChannel = key.channel();
                    Peer peer = (Peer) key.attachment();

                    if (key.isConnectable()) {
                        SocketChannel channel = (SocketChannel) selectableChannel;
                        log.info("finishing connection to {}", peer.getId());
                        try {
                            if (peers.containsKey(peer.getId())) {
                                log.info("peer already connected {}", peer.getId());
                                key.cancel();
                                continue;
                            }
                            if (channel.finishConnect()) {
                                log.info("peer connected {}", peer.getId());
                                // remove connect interest
                                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
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
                        if (selectableChannel instanceof DatagramChannel) {
                            // udp server socket
                            DatagramChannel udpSrv = (DatagramChannel) selectableChannel;
                            ByteBuffer buf = ByteBuffer.allocate(Peer.PACKET_SIZE);
                            InetSocketAddress source = (InetSocketAddress) udpSrv.receive(buf);
                            if (source == null) {
                                // some receive()s have no source address set
                                continue;
                            }
                            String id = peerID(source);
                            Peer udpPeer = peers.get(id);
                            // if the peer is null, the then sender is not a neighbor
                            if (udpPeer != null) {
                                udpPeer.read(buf);
                            }
                            continue;
                        }
                        //log.info("reading from peer {}", peer.getId());
                        if (peer.read() == -1) {
                            removeDisconnectedPeer(selectableChannel, peer.getId());
                        }
                    }

                    if (key.isWritable()) {
                        switch (peer.write()) {
                            case 0:
                                // nothing was written, probably because no message was available to send.
                                // lets unregister this channel from write interests until at least
                                // one message is back available for sending.
                                if (selectableChannel instanceof SocketChannel) {
                                    key.interestOps(SelectionKey.OP_READ);
                                } else {
                                    // as udp peer 'connections' are write only, we de-register any interests
                                    // and re-add the write interest once a message is available to send
                                    key.interestOps(0);
                                }
                                break;
                            case -1:
                                removeDisconnectedPeer(selectableChannel, peer.getId());
                                break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("gossip protocol stopped");
        }
    }

    private static LongHashFunction txCacheDigestHashFunc = LongHashFunction.xx();

    public static long getTxCacheDigest(byte[] receivedData) throws NoSuchAlgorithmException {
        return txCacheDigestHashFunc.hashBytes(receivedData);
    }

    private void removeDisconnectedPeer(SelectableChannel channel, String id) throws IOException {
        log.info("removing peer {} from connected peers", id);
        channel.close();
        peers.remove(id);
    }

    private String peerID(SocketChannel peerChannel) throws IOException {
        InetSocketAddress socketAddr = (InetSocketAddress) peerChannel.getRemoteAddress();
        return socketAddr.getAddress().getHostAddress();
    }

    private String peerID(InetSocketAddress addr) throws IOException {
        return addr.getAddress().getHostAddress();
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


    public BlockingQueue<Triple<Peer, TransactionViewModel, Boolean>> getReceivedStageQueue() {
        return receivedStageQueue;
    }

    public BlockingQueue<Pair<Peer, TransactionViewModel>> getBroadcastStageQueue() {
        return broadcastStageQueue;
    }

    public BlockingQueue<Pair<Peer, Hash>> getReplyStageQueue() {
        return replyStageQueue;
    }

    public BlockingQueue<Triple<Peer, Triple<byte[], Long, Hash>, Boolean>> getTxValidationStageQueue() {
        return txValidationStageQueue;
    }

    public void shutdown() {
        SHUTDOWN.set(true);
    }
}
