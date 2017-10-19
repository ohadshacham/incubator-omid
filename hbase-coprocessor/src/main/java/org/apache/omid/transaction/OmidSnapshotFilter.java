/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.transaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTable;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.CompactorScanner;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.omid.committable.hbase.HBaseCommitTableConfig.COMMIT_TABLE_NAME_KEY;

/**
 * A coprocessor for filtering the snapshot at the server side instead of copying redudant data to the client
 * side and filter there. This coprocessor can also saves rpcs in case a reread of shadow cell is required. 
 */
public class OmidSnapshotFilter extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidSnapshotFilter.class);

    private static final String HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_KEY
            = "omid.hbase.compactor.retain.tombstones";
    private static final boolean HBASE_RETAIN_NON_TRANSACTIONALLY_DELETED_CELLS_DEFAULT = true;

    final static String OMID_COMPACTABLE_CF_FLAG = "OMID_ENABLED";

    private HBaseCommitTableConfig commitTableConf = null;
    private Configuration conf = null;
    @VisibleForTesting
    Queue<CommitTable.Client> commitTableClientQueue = new ConcurrentLinkedQueue<>();


    public OmidSnapshotFilter() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting filtering coprocessor");
        conf = env.getConfiguration();
        commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = conf.get(COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            commitTableConf.setTableName(commitTableName);
        }
        LOG.info("Filtering coprocessor started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping filtering coprocessor");
        if (commitTableClientQueue != null) {
            for (CommitTable.Client commitTableClient : commitTableClientQueue) {
                commitTableClient.close();
            }
        }
        LOG.info("Filtering coprocessor stopped");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
      throws IOException {

//        long id = Bytes.toLong(get.getAttribute("TRANSACTION_ID"));
//        long readTs = Bytes.toLong(get.getAttribute("TRANSACTION_READ_TS"));

        TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(get.getAttribute(CellUtils.TRANSACTION_ATTRIBUTE));

        Result result = e.getEnvironment().getRegion().get(get);

        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterCellsForSnapshot(result.listCells(), transaction, get.getMaxVersions(), new HashMap<String, List<Cell>>());
        }

        Collections.copy(results, filteredKeyValues);

    }


//      Transaction tx = getFromOperation(get);
//      if (tx != null) {
//        projectFamilyDeletes(get);
//        get.setMaxVersions();
//        get.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
//                         TxUtils.getMaxVisibleTimestamp(tx));
//        Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, get.getFilter());
//        get.setFilter(newFilter);
//      }
//    }

//    @Override
//    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
//      throws IOException {
//      Transaction tx = getFromOperation(scan);
//      if (tx != null) {
//        projectFamilyDeletes(scan);
//        scan.setMaxVersions();
//        scan.setTimeRange(TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx, readNonTxnData),
//                          TxUtils.getMaxVisibleTimestamp(tx));
//        Filter newFilter = getTransactionFilter(tx, ScanType.USER_SCAN, scan.getFilter());
//        scan.setFilter(newFilter);
//      }
//      return s;
//    }

    
    Optional<Long> tryToLocateCellCommitTimestamp(transaction.getEpoch(), kv,
            commitCache) {
        
    }
    
    private Optional<Long> getCommitTimestamp(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
            throws IOException {

        long startTimestamp = transaction.getStartTimestamp();

        if (kv.getTimestamp() == startTimestamp) {
            return Optional.of(startTimestamp);
        }

        return tryToLocateCellCommitTimestamp(transaction.getTransactionManager(), transaction.getEpoch(), kv,
                commitCache);
    }
   
    private boolean checkFamilyDeletionCache(Cell cell, HBaseTransaction transaction, Map<String, List<Cell>> familyDeletionCache, Map<Long, Long> commitCache) throws IOException {
        List<Cell> familyDeletionCells = familyDeletionCache.get(Bytes.toString((cell.getRow())));
        if (familyDeletionCells != null) {
            for(Cell familyDeletionCell : familyDeletionCells) {
                String family = Bytes.toString(cell.getFamily());
                String familyDeletion = Bytes.toString(familyDeletionCell.getFamily());
                if (family.equals(familyDeletion)) {
                    Optional<Long> familyDeletionCommitTimestamp = getCommitTimestamp(familyDeletionCell, transaction, commitCache);
                    if (familyDeletionCommitTimestamp.isPresent() && familyDeletionCommitTimestamp.get() >= cell.getTimestamp()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    List<Cell> filterCellsForSnapshot(List<Cell> rawCells, TSOProto.Transaction transaction,
            int versionsToRequest, Map<String, List<Cell>> familyDeletionCache) throws IOException {

        long id = transaction.getTimestamp();
        long readTs = transaction.getReadTimestamp();
        VisibilityLevel visibilityLevel = VisibilityLevel.fromInteger(transaction.getVisibilityLevel());

        assert (rawCells != null && id > 0 && versionsToRequest >= 1);

        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        List<Get> pendingGetsList = new ArrayList<>();

        int numberOfVersionsToFetch = versionsToRequest * 2;
        if (numberOfVersionsToFetch < 1) {
            numberOfVersionsToFetch = versionsToRequest;
        }

        Map<Long, Long> commitCache = TTable.buildCommitCache(rawCells);
//        SortedMap<Cell, Optional<Cell>> cellToSc = CellUtils.mapCellsToShadowCells(rawCells);
        
        TTable.buildFamilyDeletionCache(rawCells, familyDeletionCache);

        for (Collection<Cell> columnCells : TTable.groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(rawCells)) {
            boolean snapshotValueFound = false;
            Cell oldestCell = null;
            for (Cell cell : columnCells) {
                snapshotValueFound = checkFamilyDeletionCache(cell, transaction, familyDeletionCache, commitCache);

                if (snapshotValueFound == true) {
                    if (visibilityLevel == VisibilityLevel.SNAPSHOT_ALL) {
                        snapshotValueFound = false;
                    } else {
                        break;
                    }
                }

                if (isCellInTransaction(cell, transaction, commitCache) ||
                        isCellInSnapshot(cell, transaction, commitCache)) {
                    if (!CellUtil.matchingValue(cell, CellUtils.DELETE_TOMBSTONE)) {
                        keyValuesInSnapshot.add(cell);
                    }

                    // We can finish looking for additional results in two cases:
                    // 1. if we found a result and we are not in SNAPSHOT_ALL mode.
                    // 2. if we found a result that was not written by the current transaction.
                    if (visibilityLevel != VisibilityLevel.SNAPSHOT_ALL ||
                            !isCellInTransaction(cell, transaction, commitCache)) {
                        snapshotValueFound = true;
                        break;
                    }
                }
                oldestCell = cell;
            }
            if (!snapshotValueFound) {
                assert (oldestCell != null);
                Get pendingGet = createPendingGet(oldestCell, numberOfVersionsToFetch);
                pendingGetsList.add(pendingGet);
            }
        }

        if (!pendingGetsList.isEmpty()) {
            Result[] pendingGetsResults = table.get(pendingGetsList);
            for (Result pendingGetResult : pendingGetsResults) {
                if (!pendingGetResult.isEmpty()) {
                    keyValuesInSnapshot.addAll(
                            filterCellsForSnapshot(pendingGetResult.listCells(), transaction, numberOfVersionsToFetch, familyDeletionCache));
                }
            }
        }

        Collections.sort(keyValuesInSnapshot, KeyValue.COMPARATOR);

        return keyValuesInSnapshot;
    }
}
