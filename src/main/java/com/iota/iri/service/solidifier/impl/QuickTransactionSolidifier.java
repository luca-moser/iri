package com.iota.iri.service.solidifier.impl;

import com.iota.iri.TransactionValidator;
import com.iota.iri.conf.SolidificationConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.solidifier.TransactionSolidifier;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.thread.DedicatedScheduledExecutorService;
import com.iota.iri.utils.thread.SilentScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link QuickTransactionSolidifier} starts at the defined depth parameter to walk up the graph towards the tips
 * and executes the {@link TransactionValidator#quietQuickSetSolid(TransactionViewModel)} on each traversed transaction.
 */
public class QuickTransactionSolidifier implements TransactionSolidifier {

    private static final Logger log = LoggerFactory.getLogger(QuickTransactionSolidifier.class);

    private final SilentScheduledExecutorService executorService = new DedicatedScheduledExecutorService(
            "Quick Transaction Solidifier", log);

    // external
    private SolidificationConfig solidificationConfig;
    private Tangle tangle;
    private SnapshotProvider snapshotProvider;
    private LatestMilestoneTracker latestMilestoneTracker;
    private TransactionValidator transactionValidator;

    /**
     * <p>
     * This method initializes the instance and registers its dependencies.
     * </p>
     * <p>
     * It simply stores the passed in values in their corresponding private properties.
     * </p>
     * <p>
     * Note: Instead of handing over the dependencies in the constructor, we register them lazy. This allows us to have
     *       circular dependencies because the instantiation is separated from the dependency injection. To reduce the
     *       amount of code that is necessary to correctly instantiate this class, we return the instance itself which
     *       allows us to still instantiate, initialize and assign in one line - see Example:
     * </p>
     *       {@code quickTransactionSolidifier = new QuickTransactionSolidifier().init(...);}
     *
     * @param solidificationConfig the configuration holding parameters for solidification
     * @param tangle Tangle object which acts as a database interface
     * @param snapshotProvider data provider for the snapshots that are relevant for the node
     * @param latestMilestoneTracker the tracker holding the latest known milestone index
     * @param transactionValidator the TransactionValidator which holds the logic for checking quick solidification
     * @return the initialized instance itself to allow chaining
     */
    public QuickTransactionSolidifier init(SolidificationConfig solidificationConfig, Tangle tangle, SnapshotProvider snapshotProvider,
            LatestMilestoneTracker latestMilestoneTracker, TransactionValidator transactionValidator) {
        this.solidificationConfig = solidificationConfig;
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.transactionValidator = transactionValidator;
        return this;
    }

    @Override
    public void start() {
        executorService.silentScheduleWithFixedDelay(this::solidify, 0,
                solidificationConfig.getSolidifierIntervalMillisec(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void solidify() {
        int latestMilestoneIndex = latestMilestoneTracker.getLatestMilestoneIndex();
        int depth = solidificationConfig.getSolidifierDepth();

        // don't solidify if not enough milestones were issued yet
        if (latestMilestoneIndex - depth <= 0) {
            return;
        }

        // we only try to quick solidify transactions when we are above the defined solidification depth
        if (snapshotProvider.getLatestSnapshot().getIndex() < latestMilestoneIndex - depth) {
            return;
        }

        DAGHelper dagHelper = DAGHelper.get(tangle);
        long start = System.currentTimeMillis();
        try {
            MilestoneViewModel milestone = MilestoneViewModel.get(tangle, latestMilestoneIndex - depth);
            // if we don't have the milestone which we wanted to use as a starting point, we simply don't run.
            if (milestone == null) {
                return;
            }
            AtomicInteger updated = new AtomicInteger();
            AtomicInteger traversed = new AtomicInteger();
            dagHelper.traverseApprovers(milestone.getHash(), tvm -> !Thread.currentThread().isInterrupted(), tvm -> {
                try {
                    if (transactionValidator.quietQuickSetSolid(tvm)) {
                        tvm.update(tangle, snapshotProvider.getInitialSnapshot(), "solid|height");
                        updated.incrementAndGet();
                    }
                    traversed.incrementAndGet();
                } catch (Exception e) {
                    log.error("error while trying to quick set solid transaction {}. reason: {}", tvm.getHash(),
                            e.getMessage());
                }
            });

            log.info("updated {} and traversed {} transactions, took {} ms", updated.get(), traversed.get(),
                    System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("error occurred during quick solidification run: {}", e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }
}
