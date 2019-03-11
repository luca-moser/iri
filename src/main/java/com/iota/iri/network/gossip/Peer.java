package com.iota.iri.network.gossip;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Peer {

    enum ConnectType {
        TCP, UDP
    }

    private static final Logger log = LoggerFactory.getLogger(Peer.class);

    // TODO: use constant for buf size
    public final static int PACKET_SIZE = 1650;

    private ByteBuffer current = ByteBuffer.allocate(PACKET_SIZE);

    // next stage in the processing of incoming data
    private BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue;

    // data to be written out to the peer
    private BlockingQueue<ByteBuffer> sendQueue = new LinkedBlockingQueue<>();
    private ByteBuffer currentToWrite;
    private long msgsWritten;
    private long msgsRead;

    private String id;
    private ConnectType connectType;
    private SelectableChannel channel;
    private Selector selector;


    public Peer(Selector selector, SelectableChannel channel, String id, ConnectType connectType, BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue) {
        this.id = id;
        this.selector = selector;
        this.connectType = connectType;
        this.channel = channel;
        this.preProcessStageQueue = preProcessStageQueue;
    }

    public int read() throws IOException, InterruptedException {
        int bytesRead = ((ReadableByteChannel) channel).read(current);
        if (current.remaining() != 0) {
            return bytesRead;
        }
        submitToProcessStage();
        return bytesRead;
    }

    public int read(ByteBuffer data) throws InterruptedException {
        data.flip();
        int bytesRead = data.limit();
        current.put(data);
        if (current.remaining() != 0) {
            return bytesRead;
        }
        submitToProcessStage();
        return bytesRead;
    }

    private void submitToProcessStage() throws InterruptedException {
        msgsRead++;
        current.flip();
        preProcessStageQueue.put(new ImmutablePair<>(this, current));
        current = ByteBuffer.allocate(PACKET_SIZE);
    }

    public int write() throws IOException {

        // previous message wasn't fully sent yet
        if (currentToWrite != null) {
            return writeMsg();
        }

        currentToWrite = sendQueue.poll();
        if (currentToWrite == null) {
            return 0;
        }
        return writeMsg();
    }

    private int writeMsg() throws IOException {
        int written = 0;
        try {
            written = ((WritableByteChannel) channel).write(currentToWrite);
        } catch (PortUnreachableException ex) {
            // thrown when a send is done to a non reachable port while using UDP
            return 0;
        }
        if (!currentToWrite.hasRemaining()) {
            msgsWritten++;
            currentToWrite = null;
        }
        return written;
    }

    public String getId() {
        return id;
    }

    public void send(ByteBuffer buf) {
        try {
            // re-register write interest
            SelectionKey key = channel.keyFor(selector);
            if (key != null && key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                if (channel instanceof DatagramChannel) {
                    key.interestOps(SelectionKey.OP_WRITE);
                } else {
                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                }
                selector.wakeup();
            }

            sendQueue.put(buf);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getMsgsWritten() {
        return msgsWritten;
    }

    public long getMsgsRead() {
        return msgsRead;
    }
}
