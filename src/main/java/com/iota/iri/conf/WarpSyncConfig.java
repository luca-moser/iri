package com.iota.iri.conf;

public interface WarpSyncConfig extends Config {

    /**
     * @return {@link Descriptions#WARP_SYNC_DELTA_THRESHOLD}
     */
    int getWarpSyncDeltaThreshold();

    interface Descriptions {

        String WARP_SYNC_DELTA_THRESHOLD = "Sets the delta from the LSM to the LM up on which "
                + "warp syncing gets automatically triggered";
    }
}
