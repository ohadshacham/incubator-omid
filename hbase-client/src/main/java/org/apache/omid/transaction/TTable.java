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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.committable.CommitTable.CommitTimestamp;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.HBaseTransactionManager.CommitTimestampLocatorImpl;
import org.apache.omid.tso.client.OmidClientConfiguration.ConflictDetectionLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Provides transactional methods for accessing and modifying a given snapshot of data identified by an opaque {@link
 * Transaction} object. It mimics the behavior in {@link org.apache.hadoop.hbase.client.HTableInterface}
 */
public class TTable implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(TTable.class);

    private final HTableInterface healerTable;

    private HTableInterface table;

    // ----------------------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------------------

    public TTable(Configuration conf, byte[] tableName) throws IOException {
        this(new HTable(conf, tableName));
    }

    public TTable(String tableName) throws IOException {
        this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
    }

    public TTable(Configuration conf, String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    public TTable(HTableInterface hTable) throws IOException {
        table = hTable;
        healerTable = new HTable(table.getConfiguration(), table.getTableName());
    }

    public TTable(HTableInterface hTable, HTableInterface healerTable) throws IOException {
        table = hTable;
        this.healerTable = healerTable;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Closeable implementation
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Releases any resources held or pending changes in internal buffers.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    @Override
    public void close() throws IOException {
        table.close();
        healerTable.close();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Transactional operations
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Transactional version of {@link HTableInterface#get(Get get)}
     *
     * @param get an instance of Get
     * @param tx  an instance of transaction to be used
     * @return Result an instance of Result
     * @throws IOException if a remote or network exception occurs.
     */
    public Result get(Transaction tx, final Get get) throws IOException {

        throwExceptionIfOpSetsTimerange(get);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long readTimestamp = transaction.getReadTimestamp();
        final Get tsget = new Get(get.getRow()).setFilter(get.getFilter());
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin();
        long endTime = Math.min(timeRange.getMax(), readTimestamp + 1);
        tsget.setTimeRange(startTime, endTime).setMaxVersions(1);
        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                    tsget.addColumn(family, CellUtils.addShadowCellSuffix(qualifier));
                }
                tsget.addColumn(family, CellUtils.FAMILY_DELETE_QUALIFIER);
                tsget.addColumn(family, CellUtils.addShadowCellSuffix(CellUtils.FAMILY_DELETE_QUALIFIER));
            }
        }
        LOG.trace("Initial Get = {}", tsget);
                
        TSOProto.Transaction.Builder transactionBuilder = TSOProto.Transaction.newBuilder();

        transactionBuilder.setTimestamp(transaction.getTransactionId());
        transactionBuilder.setReadTimestamp(transaction.getReadTimestamp());
        transactionBuilder.setVisibilityLevel(transaction.getVisibilityLevel().ordinal());
                
        tsget.setAttribute(CellUtils.TRANSACTION_ATTRIBUTE, transactionBuilder.build().toByteArray());
        
        // Return the KVs that belong to the transaction snapshot, ask for more
        // versions if needed
        Result result = table.get(tsget);
        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterCellsForSnapshot(result.listCells(), transaction, tsget.getMaxVersions(), new HashMap<String, List<Cell>>());
        }

        return Result.create(filteredKeyValues);
    }

    private void familyQualifierBasedDeletion(HBaseTransaction tx, Put deleteP, Get deleteG) throws IOException {
        Result result = this.get(tx, deleteG);
        if (!result.isEmpty()) {
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap()
                    .entrySet()) {
                byte[] family = entryF.getKey();
                for (Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                    byte[] qualifier = entryQ.getKey();
                    tx.addWriteSetElement(new HBaseCellId(table, deleteP.getRow(), family, qualifier,
                            tx.getWriteTimestamp()));
                }
                deleteP.add(family, CellUtils.FAMILY_DELETE_QUALIFIER, tx.getWriteTimestamp(),
                        HConstants.EMPTY_BYTE_ARRAY);
            }
        }
    }

    private void  familyQualifierBasedDeletionWithOutRead(HBaseTransaction tx, Put deleteP, Get deleteG) {
        Set<byte[]> fset = deleteG.getFamilyMap().keySet();

        for (byte[] family : fset) {
            deleteP.add(family, CellUtils.FAMILY_DELETE_QUALIFIER, tx.getWriteTimestamp(),
                    HConstants.EMPTY_BYTE_ARRAY);
        }
        tx.addWriteSetElement(new HBaseCellId(table, deleteP.getRow(), null, null,
                tx.getWriteTimestamp()));
    }

    /**
     * Transactional version of {@link HTableInterface#delete(Delete delete)}
     *
     * @param delete an instance of Delete
     * @param tx     an instance of transaction to be used
     * @throws IOException if a remote or network exception occurs.
     */
    public void delete(Transaction tx, Delete delete) throws IOException {

        throwExceptionIfOpSetsTimerange(delete);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long writeTimestamp = transaction.getStartTimestamp();
        boolean deleteFamily = false;

        final Put deleteP = new Put(delete.getRow(), writeTimestamp);
        final Get deleteG = new Get(delete.getRow());
        Map<byte[], List<Cell>> fmap = delete.getFamilyCellMap();
        if (fmap.isEmpty()) {
            familyQualifierBasedDeletion(transaction, deleteP, deleteG);
        }

        for (List<Cell> cells : fmap.values()) {
            for (Cell cell : cells) {
                CellUtils.validateCell(cell, writeTimestamp);
                switch (KeyValue.Type.codeToType(cell.getTypeByte())) {
                    case DeleteColumn:
                        deleteP.add(CellUtil.cloneFamily(cell),
                                    CellUtil.cloneQualifier(cell),
                                    writeTimestamp,
                                    CellUtils.DELETE_TOMBSTONE);
                        transaction.addWriteSetElement(
                            new HBaseCellId(table,
                                            delete.getRow(),
                                            CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell),
                                            writeTimestamp));
                        break;
                    case DeleteFamily:
                        deleteG.addFamily(CellUtil.cloneFamily(cell));
                        deleteFamily = true;
                        break;
                    case Delete:
                        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                            deleteP.add(CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        writeTimestamp,
                                        CellUtils.DELETE_TOMBSTONE);
                            transaction.addWriteSetElement(
                                new HBaseCellId(table,
                                                delete.getRow(),
                                                CellUtil.cloneFamily(cell),
                                                CellUtil.cloneQualifier(cell),
                                                writeTimestamp));
                            break;
                        } else {
                            throw new UnsupportedOperationException(
                                "Cannot delete specific versions on Snapshot Isolation.");
                        }
                    default:
                        break;
                }
            }
        }
        if (deleteFamily) {
            if (enforceHBaseTransactionManagerAsParam(transaction.getTransactionManager()).getConflictDetectionLevel() == ConflictDetectionLevel.ROW) {
                familyQualifierBasedDeletionWithOutRead(transaction, deleteP, deleteG);
            } else {
                familyQualifierBasedDeletion(transaction, deleteP, deleteG);
            }
        }

        if (!deleteP.isEmpty()) {
            table.put(deleteP);
        }

    }

    /**
     * Transactional version of {@link HTableInterface#put(Put put)}
     *
     * @param put an instance of Put
     * @param tx  an instance of transaction to be used
     * @throws IOException if a remote or network exception occurs.
     */
    public void put(Transaction tx, Put put) throws IOException {

        throwExceptionIfOpSetsTimerange(put);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        final long writeTimestamp = transaction.getWriteTimestamp();

        // create put with correct ts
        final Put tsput = new Put(put.getRow(), writeTimestamp);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                CellUtils.validateCell(c, writeTimestamp);
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), writeTimestamp);
                tsput.add(kv);

                transaction.addWriteSetElement(
                    new HBaseCellId(table,
                                    CellUtil.cloneRow(kv),
                                    CellUtil.cloneFamily(kv),
                                    CellUtil.cloneQualifier(kv),
                                    kv.getTimestamp()));
            }
        }

        table.put(tsput);
    }

    /**
     * Transactional version of {@link HTableInterface#getScanner(Scan scan)}
     *
     * @param scan an instance of Scan
     * @param tx   an instance of transaction to be used
     * @return ResultScanner an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs.
     */
    public ResultScanner getScanner(Transaction tx, Scan scan) throws IOException {

        throwExceptionIfOpSetsTimerange(scan);

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        Scan tsscan = new Scan(scan);
        tsscan.setMaxVersions(1);
        tsscan.setTimeRange(0, transaction.getReadTimestamp() + 1);
        Map<byte[], NavigableSet<byte[]>> kvs = scan.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null) {
                continue;
            }
            for (byte[] qualifier : qualifiers) {
                tsscan.addColumn(family, CellUtils.addShadowCellSuffix(qualifier));
            }
            if (!qualifiers.isEmpty()) {
                scan.addColumn(entry.getKey(), CellUtils.FAMILY_DELETE_QUALIFIER);
            }
        }
        return new TransactionalClientScanner(transaction, tsscan, 1);
    }

    /**
     * Check whether a cell was deleted using family deletion marker
     *
     * @param cell                The cell to check
     * @param transaction         Defines the current snapshot
     * @param familyDeletionCache Accumulates the family deletion markers to identify cells that deleted with a higher version
     * @param commitCache         Holds shadow cells information
     * @return Whether the cell was deleted
     */
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
    /**
     * Filters the raw results returned from HBase and returns only those belonging to the current snapshot, as defined
     * by the transaction object. If the raw results don't contain enough information for a particular qualifier, it
     * will request more versions from HBase.
     *
     * @param rawCells          Raw cells that we are going to filter
     * @param transaction       Defines the current snapshot
     * @param versionsToRequest Number of versions requested from hbase
     * @param familyDeletionCache Accumulates the family deletion markers to identify cells that deleted with a higher version
     * @return Filtered KVs belonging to the transaction snapshot
     */
    List<Cell> filterCellsForSnapshot(List<Cell> rawCells, HBaseTransaction transaction,
                                      int versionsToRequest, Map<String, List<Cell>> familyDeletionCache) throws IOException {

        assert (rawCells != null && transaction != null && versionsToRequest >= 1);

        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        List<Get> pendingGetsList = new ArrayList<>();

        int numberOfVersionsToFetch = versionsToRequest * 2;
        if (numberOfVersionsToFetch < 1) {
            numberOfVersionsToFetch = versionsToRequest;
        }

        Map<Long, Long> commitCache = buildCommitCache(rawCells);
        buildFamilyDeletionCache(rawCells, familyDeletionCache);

        for (Collection<Cell> columnCells : groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(rawCells)) {
            boolean snapshotValueFound = false;
            Cell oldestCell = null;
            for (Cell cell : columnCells) {
                snapshotValueFound = checkFamilyDeletionCache(cell, transaction, familyDeletionCache, commitCache);

                if (snapshotValueFound == true) {
                    if (transaction.getVisibilityLevel() == VisibilityLevel.SNAPSHOT_ALL) {
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
                    if (transaction.getVisibilityLevel() != VisibilityLevel.SNAPSHOT_ALL ||
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

    public static Map<Long, Long> buildCommitCache(List<Cell> rawCells) {

        Map<Long, Long> commitCache = new HashMap<>();

        for (Cell cell : rawCells) {
            if (CellUtils.isShadowCell(cell)) {
                commitCache.put(cell.getTimestamp(), Bytes.toLong(CellUtil.cloneValue(cell)));
            }
        }

        return commitCache;
    }

    public static void buildFamilyDeletionCache(List<Cell> rawCells, Map<String, List<Cell>> familyDeletionCache) {

        for (Cell cell : rawCells) {
            if (CellUtil.matchingQualifier(cell, CellUtils.FAMILY_DELETE_QUALIFIER) &&
                    CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY)) {

                String row = Bytes.toString(cell.getRow());
                List<Cell> cells = familyDeletionCache.get(row);
                if (cells == null) {
                    cells = new ArrayList<>();
                    familyDeletionCache.put(row, cells);
                }

                cells.add(cell);
            }
        }

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

    private boolean isCellInTransaction(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache) {

        long startTimestamp = transaction.getStartTimestamp();
        long readTimestamp = transaction.getReadTimestamp();

        // A cell was written by a transaction if its timestamp is larger than its startTimestamp and smaller or equal to its readTimestamp.
        // There also might be a case where the cell was written by the transaction and its timestamp equals to its writeTimestamp, however,
        // this case occurs after checkpoint and in this case we do not want to read this data.
        if (kv.getTimestamp() >= startTimestamp && kv.getTimestamp() <= readTimestamp) {
            return true;
        }

        return false;
    }

    private boolean isCellInSnapshot(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
        throws IOException {

        Optional<Long> commitTimestamp = getCommitTimestamp(kv, transaction, commitCache);

        return commitTimestamp.isPresent() && commitTimestamp.get() < transaction.getStartTimestamp();
    }

    private Get createPendingGet(Cell cell, int versionCount) throws IOException {

        Get pendingGet = new Get(CellUtil.cloneRow(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                                                                                       cell.getQualifierOffset(),
                                                                                       cell.getQualifierLength()));
        pendingGet.setMaxVersions(versionCount);
        pendingGet.setTimeRange(0, cell.getTimestamp());

        return pendingGet;
    }

    private Optional<Long> tryToLocateCellCommitTimestamp(AbstractTransactionManager transactionManager,
                                                          long epoch,
                                                          Cell cell,
                                                          Map<Long, Long> commitCache)
        throws IOException {

        CommitTimestamp tentativeCommitTimestamp =
            transactionManager.locateCellCommitTimestamp(
                cell.getTimestamp(),
                epoch,
                new CommitTimestampLocatorImpl(
                    new HBaseCellId(table,
                                    CellUtil.cloneRow(cell),
                                    CellUtil.cloneFamily(cell),
                                    CellUtil.cloneQualifier(cell),
                                    cell.getTimestamp()),
                    commitCache));

        // If transaction that added the cell was invalidated
        if (!tentativeCommitTimestamp.isValid()) {
            return Optional.absent();
        }

        switch (tentativeCommitTimestamp.getLocation()) {
            case COMMIT_TABLE:
                // If the commit timestamp is found in the persisted commit table,
                // that means the writing process of the shadow cell in the post
                // commit phase of the client probably failed, so we heal the shadow
                // cell with the right commit timestamp for avoiding further reads to
                // hit the storage
                healShadowCell(cell, tentativeCommitTimestamp.getValue());
                return Optional.of(tentativeCommitTimestamp.getValue());
            case CACHE:
            case SHADOW_CELL:
                return Optional.of(tentativeCommitTimestamp.getValue());
            case NOT_PRESENT:
                return Optional.absent();
            default:
                assert (false);
                return Optional.absent();
        }
    }

    void healShadowCell(Cell cell, long commitTimestamp) {
        Put put = new Put(CellUtil.cloneRow(cell));
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] shadowCellQualifier = CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                                                                   cell.getQualifierOffset(),
                                                                   cell.getQualifierLength());
        put.add(family, shadowCellQualifier, cell.getTimestamp(), Bytes.toBytes(commitTimestamp));
        try {
            healerTable.put(put);
        } catch (IOException e) {
            LOG.warn("Failed healing shadow cell for kv {}", cell, e);
        }
    }

    protected class TransactionalClientScanner implements ResultScanner {

        private HBaseTransaction state;
        private ResultScanner innerScanner;
        private int maxVersions;
        Map<String, List<Cell>> familyDeletionCache;

        TransactionalClientScanner(HBaseTransaction state, Scan scan, int maxVersions)
            throws IOException {
            this.state = state;
            this.innerScanner = table.getScanner(scan);
            this.maxVersions = maxVersions;
            this.familyDeletionCache = new HashMap<String, List<Cell>>();
        }


        @Override
        public Result next() throws IOException {
            List<Cell> filteredResult = Collections.emptyList();
            while (filteredResult.isEmpty()) {
                Result result = innerScanner.next();
                if (result == null) {
                    return null;
                }
                if (!result.isEmpty()) {
                    filteredResult = filterCellsForSnapshot(result.listCells(), state, maxVersions, familyDeletionCache);
                }
            }
            return Result.create(filteredResult);
        }

        // In principle no need to override, copied from super.next(int) to make
        // sure it works even if super.next(int)
        // changes its implementation
        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

        @Override
        public void close() {
            innerScanner.close();
        }

        @Override
        public Iterator<Result> iterator() {
            return new ResultIterator(this);
        }

        // ------------------------------------------------------------------------------------------------------------
        // --------------------------------- Helper class for TransactionalClientScanner ------------------------------
        // ------------------------------------------------------------------------------------------------------------

        class ResultIterator implements Iterator<Result> {

            TransactionalClientScanner scanner;
            Result currentResult;

            ResultIterator(TransactionalClientScanner scanner) {
                try {
                    this.scanner = scanner;
                    currentResult = scanner.next();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return currentResult != null && !currentResult.isEmpty();
            }

            @Override
            public Result next() {
                try {
                    Result result = currentResult;
                    currentResult = scanner.next();
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }

        }

    }

    /**
     * Delegates to {@link HTable#getTableName()}
     *
     * @return array of byte
     */
    public byte[] getTableName() {
        return table.getTableName();
    }

    /**
     * Delegates to {@link HTable#getConfiguration()}
     *
     * @return standard configuration object
     */
    public Configuration getConfiguration() {
        return table.getConfiguration();
    }

    /**
     * Delegates to {@link HTable#getTableDescriptor()}
     *
     * @return HTableDescriptor an instance of HTableDescriptor
     * @throws IOException if a remote or network exception occurs.
     */
    public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
    }

    /**
     * Transactional version of {@link HTableInterface#exists(Get get)}
     *
     * @param transaction an instance of transaction to be used
     * @param get         an instance of Get
     * @return true if cell exists
     * @throws IOException if a remote or network exception occurs.
     */
    public boolean exists(Transaction transaction, Get get) throws IOException {
        Result result = get(transaction, get);
        return !result.isEmpty();
    }

    /* TODO What should we do with this methods???
     * @Override public void batch(Transaction transaction, List<? extends Row>
     * actions, Object[] results) throws IOException, InterruptedException {}
     *
     * @Override public Object[] batch(Transaction transaction, List<? extends
     * Row> actions) throws IOException, InterruptedException {}
     *
     * @Override public <R> void batchCallback(Transaction transaction, List<?
     * extends Row> actions, Object[] results, Callback<R> callback) throws
     * IOException, InterruptedException {}
     *
     * @Override public <R> Object[] batchCallback(List<? extends Row> actions,
     * Callback<R> callback) throws IOException, InterruptedException {}
     */

    /**
     * Transactional version of {@link HTableInterface#get(List gets)}
     *
     * @param transaction an instance of transaction to be used
     * @param gets        list of Get instances
     * @return array of Results
     * @throws IOException if a remote or network exception occurs
     */
    public Result[] get(Transaction transaction, List<Get> gets) throws IOException {
        Result[] results = new Result[gets.size()];
        int i = 0;
        for (Get get : gets) {
            results[i++] = get(transaction, get);
        }
        return results;
    }

    /**
     * Transactional version of {@link HTableInterface#getScanner(byte[] family)}
     *
     * @param transaction an instance of transaction to be used
     * @param family      column family
     * @return an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(transaction, scan);
    }

    /**
     * Transactional version of {@link HTableInterface#getScanner(byte[] family, byte[] qualifier)}
     *
     * @param transaction an instance of transaction to be used
     * @param family      column family
     * @param qualifier   column name
     * @return an instance of ResultScanner
     * @throws IOException if a remote or network exception occurs
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family, byte[] qualifier)
        throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(transaction, scan);
    }

    /**
     * Transactional version of {@link HTableInterface#put(List puts)}
     *
     * @param transaction an instance of transaction to be used
     * @param puts        List of puts
     * @throws IOException if a remote or network exception occurs
     */
    public void put(Transaction transaction, List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(transaction, put);
        }
    }

    /**
     * Transactional version of {@link HTableInterface#delete(List deletes)}
     *
     * @param transaction an instance of transaction to be used
     * @param deletes        List of deletes
     * @throws IOException if a remote or network exception occurs
     */
    public void delete(Transaction transaction, List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(transaction, delete);
        }
    }

    /**
     * Provides access to the underliying HTable in order to configure it or to perform unsafe (non-transactional)
     * operations. The latter would break the transactional guarantees of the whole system.
     *
     * @return The underlying HTable object
     */
    public HTableInterface getHTable() {
        return table;
    }

    /**
     * Delegates to {@link HTable#setAutoFlush(boolean autoFlush)}
     */
    public void setAutoFlush(boolean autoFlush) {
        table.setAutoFlush(autoFlush, true);
    }

    /**
     * Delegates to {@link HTable#isAutoFlush()}
     */
    public boolean isAutoFlush() {
        return table.isAutoFlush();
    }

    /**
     * Delegates to see HTable.getWriteBufferSize()
     */
    public long getWriteBufferSize() {
        return table.getWriteBufferSize();
    }

    /**
     * Delegates to see HTable.setWriteBufferSize()
     */
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        table.setWriteBufferSize(writeBufferSize);
    }

    /**
     * Delegates to see HTable.flushCommits()
     */
    public void flushCommits() throws IOException {
        table.flushCommits();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    private void throwExceptionIfOpSetsTimerange(Get getOperation) {
        TimeRange tr = getOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void throwExceptionIfOpSetsTimerange(Scan scanOperation) {
        TimeRange tr = scanOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void checkTimerangeIsSetToDefaultValuesOrThrowException(TimeRange tr) {
        if (tr.getMin() != 0L || tr.getMax() != Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Timestamp/timerange not allowed in transactional user operations");
        }
    }

    private void throwExceptionIfOpSetsTimerange(Mutation userOperation) {
        if (userOperation.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
            throw new IllegalArgumentException(
                "Timestamp not allowed in transactional user operations");
        }
    }

    private HBaseTransaction enforceHBaseTransactionAsParam(Transaction tx) {
        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                String.format("The transaction object passed %s is not an instance of HBaseTransaction",
                              tx.getClass().getName()));
        }
    }

    private HBaseTransactionManager enforceHBaseTransactionManagerAsParam(TransactionManager tm) {
        if (tm instanceof HBaseTransactionManager) {
            return (HBaseTransactionManager) tm;
        } else {
            throw new IllegalArgumentException(
                String.format("The transaction manager object passed %s is not an instance of HBaseTransactionManager ",
                              tm.getClass().getName()));
        }
    }

    static ImmutableList<Collection<Cell>> groupCellsByColumnFilteringShadowCellsAndFamilyDeletion(List<Cell> rawCells) {

        Predicate<Cell> shadowCellAndFamilyDeletionFilter = new Predicate<Cell>() {

            @Override
            public boolean apply(Cell cell) {
                boolean familyDeletionMarkerCondition = CellUtil.matchingQualifier(cell, CellUtils.FAMILY_DELETE_QUALIFIER) &&
                                                        CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);

                return cell != null && !CellUtils.isShadowCell(cell) && !familyDeletionMarkerCondition;
            }

        };

        Function<Cell, ColumnWrapper> cellToColumnWrapper = new Function<Cell, ColumnWrapper>() {

            @Override
            public ColumnWrapper apply(Cell cell) {
                return new ColumnWrapper(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
            }

        };

        return Multimaps.index(Iterables.filter(rawCells, shadowCellAndFamilyDeletionFilter), cellToColumnWrapper)
            .asMap().values()
            .asList();
    }
}
