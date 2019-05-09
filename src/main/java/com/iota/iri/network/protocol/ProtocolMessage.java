package com.iota.iri.network.protocol;

/**
 * Defines the different message types supported by the protocol and their characteristics.
 */
public enum ProtocolMessage {
    /**
     * The message header sent in each message denoting the TLV fields and used protocol version.
     */
    HEADER((byte) 0, (short) Protocol.PROTOCOL_HEADER_BYTES_LENGTH, false),
    /**
     * The initial handshake packet sent over the wire up on a new neighbor connection. Made up of: - own server socket
     * port (2 bytes) - time at which the packet was sent (8 bytes) - own used byte encoded coordinator address (49
     * bytes) - own used MWM (1 byte)
     */
    HANDSHAKE((byte) 1, (short) 60, false),
    /**
     * The transaction payload + requested transaction hash gossipping packet. In reality most of this packets won't
     * take up their full 1604 bytes as the signature message fragment of the tx is truncated.
     */
    TRANSACTION_GOSSIP((byte) 2,
            (short) (Protocol.GOSSIP_REQUESTED_TX_HASH_BYTES_LENGTH + Protocol.NON_SIG_TX_PART_BYTES_LENGTH
                    + Protocol.SIG_DATA_MAX_BYTES_LENGTH),
            true),

    // target milestone index (4 bytes) (int)
    WARP_SYNC_REQUEST((byte) 3, (short) 4, false),
    // cancel signal
    WARP_SYNC_CANCEL((byte) 4, (short) 1, false),
    // latest known milestone index (4 bytes) + amount of transactions which will be sent (8 bytes)
    WARP_SYNC_OK((byte) 5, (byte) 12, false),
    // transaction payload, max 1504 bytes
    WARP_SYNC_TX((byte) 6, (short) (Protocol.NON_SIG_TX_PART_BYTES_LENGTH + Protocol.SIG_DATA_MAX_BYTES_LENGTH), true),
    // signal to tell the other node to start transmitting transactions
    WARP_SYNC_START((byte) 7, (short) 1, false);

    ProtocolMessage(byte typeID, short maxLength, boolean supportsDynamicLength) {
        this.typeID = typeID;
        this.maxLength = maxLength;
        this.supportsDynamicLength = supportsDynamicLength;
    }

    private static ProtocolMessage[] lookup = new ProtocolMessage[256];

    static {
        lookup[0] = ProtocolMessage.HEADER;
        lookup[1] = ProtocolMessage.HANDSHAKE;
        lookup[2] = ProtocolMessage.TRANSACTION_GOSSIP;
        lookup[3] = ProtocolMessage.WARP_SYNC_REQUEST;
        lookup[4] = ProtocolMessage.WARP_SYNC_CANCEL;
        lookup[5] = ProtocolMessage.WARP_SYNC_OK;
        lookup[6] = ProtocolMessage.WARP_SYNC_TX;
        lookup[7] = ProtocolMessage.WARP_SYNC_START;
    }

    /**
     * Gets the {@link ProtocolMessage} corresponding to the given type id.
     * 
     * @param typeID the type id of the message
     * @return the {@link ProtocolMessage} corresponding to the given type id or null
     */
    public static ProtocolMessage fromTypeID(byte typeID) {
        if (typeID >= lookup.length) {
            return null;
        }
        return lookup[typeID];
    }

    private byte typeID;
    private short maxLength;
    private boolean supportsDynamicLength;

    /**
     * Gets the type id of the message.
     * 
     * @return the type id of the message
     */
    public byte getTypeID() {
        return typeID;
    }

    /**
     * Gets the maximum length of the message.
     * 
     * @return the maximum length of the message
     */
    public short getMaxLength() {
        return maxLength;
    }

    /**
     * Whether this message type supports dynamic length.
     * 
     * @return whether this message type supports dynamic length
     */
    public boolean supportsDynamicLength() {
        return supportsDynamicLength;
    }

}
