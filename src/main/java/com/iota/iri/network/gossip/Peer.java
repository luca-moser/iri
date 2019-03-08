package com.iota.iri.network.gossip;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Peer {

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
    private SocketChannel channel;
    private Selector selector;


    public Peer(Selector selector, SocketChannel channel, String id, BlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue) {
        this.id = id;
        this.selector = selector;
        this.channel = channel;
        this.preProcessStageQueue = preProcessStageQueue;
    }

    public int read() throws IOException, InterruptedException {
        int bytesRead = channel.read(current);
        if (current.remaining() != 0) {
            return bytesRead;
        }
        msgsRead++;
        current.flip();
        preProcessStageQueue.put(new ImmutablePair<>(this, current));
        current = ByteBuffer.allocate(PACKET_SIZE);
        return bytesRead;
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
        int written = channel.write(currentToWrite);
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
            if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
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
