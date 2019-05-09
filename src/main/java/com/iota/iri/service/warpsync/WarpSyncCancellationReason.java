package com.iota.iri.service.warpsync;

public enum WarpSyncCancellationReason {
    // requested target milestone is too far back
    MILESTONE_TARGET_TOO_FAR_BACK((byte) 0),
    // used when the node doesn't have the needed data by the requested neighbor:
    // - milestone is not in the database or not solid
    // - transactions which are approved by the milestone are not in the database or solid
    // - during transmission when due to a pruning transaction data is suddenly missing
    INSUFFICIENT_DATA((byte) 1),
    // used when a node didn't send an expected msg in time
    NOT_REPLIED_IN_TIME((byte) 2),
    // used when a sending node didn't send transactions in time
    TX_RECEIVE_TIMEOUT((byte) 3),
    // used when the node is shutting down or got interrupted
    INTERRUPTED((byte) 4),
    // used when the request slow is already filled by another neighbor
    REQUEST_SLOT_FILLED((byte) 5),
    // used when the requested node is itself not synced
    NOT_SYNCED((byte) 6),
    // used when the advertised milestone from the warp sync requester
    // is newer than the node's own milestone
    MILESTONE_TARGET_TOO_NEW((byte) 7);

    private byte id;

    WarpSyncCancellationReason(byte id) {
        this.id = id;
    }

    public byte getID() {
        return id;
    }

    private static WarpSyncCancellationReason[] lookup = new WarpSyncCancellationReason[10];

    static {
        lookup[0] = WarpSyncCancellationReason.MILESTONE_TARGET_TOO_FAR_BACK;
        lookup[1] = WarpSyncCancellationReason.INSUFFICIENT_DATA;
        lookup[2] = WarpSyncCancellationReason.NOT_REPLIED_IN_TIME;
        lookup[3] = WarpSyncCancellationReason.TX_RECEIVE_TIMEOUT;
        lookup[4] = WarpSyncCancellationReason.INTERRUPTED;
        lookup[5] = WarpSyncCancellationReason.REQUEST_SLOT_FILLED;
        lookup[6] = WarpSyncCancellationReason.NOT_SYNCED;
        lookup[7] = WarpSyncCancellationReason.MILESTONE_TARGET_TOO_NEW;
    }

    public static WarpSyncCancellationReason fromValue(byte val) {
        if (val >= lookup.length) {
            return null;
        }
        return lookup[val];
    }

}
