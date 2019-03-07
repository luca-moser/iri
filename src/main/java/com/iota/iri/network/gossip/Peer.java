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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Peer {

    private static final Logger log = LoggerFactory.getLogger(Peer.class);

    // TODO: use constant for buf size
    public final static int PACKET_SIZE = 1650;

    private ByteBuffer current = ByteBuffer.allocate(PACKET_SIZE);

    // next stage in the processing of incoming data
    private ArrayBlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue;

    // data to be written out to the peer
    private ArrayBlockingQueue<ByteBuffer> writeOutQueue = new ArrayBlockingQueue<>(128);
    private ByteBuffer currentToWrite;

    private String id;
    private SocketChannel channel;
    private Selector selector;

    public Peer(Selector selector, SocketChannel channel, String id, ArrayBlockingQueue<Pair<Peer, ByteBuffer>> preProcessStageQueue) {
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

        try {
            currentToWrite = writeOutQueue.poll(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (currentToWrite == null) {
            return 0;
        }
        return writeMsg();
    }

    private int writeMsg() throws IOException {
        int written = channel.write(currentToWrite);
        if (!currentToWrite.hasRemaining()) {
            currentToWrite = null;
        }
        return written;
    }

    public String getId() {
        return id;
    }

    public void send(ByteBuffer buf) {
        try {
            if (writeOutQueue.size() == 0) {
                // channel key likely to be set to read key only
                SelectionKey key = channel.keyFor(selector);
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                selector.wakeup();
            }
            writeOutQueue.put(buf);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
