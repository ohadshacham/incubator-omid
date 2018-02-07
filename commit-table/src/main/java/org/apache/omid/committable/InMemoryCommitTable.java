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
package org.apache.omid.committable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.omid.committable.CommitTable.CommitTimestamp.Location;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCommitTable implements CommitTable {

    final ConcurrentHashMap<Long, Long> table = new ConcurrentHashMap<>();

    long lowWatermark;

    @Override
    public CommitTable.Writer getWriter() {
        return new Writer();
    }

    @Override
    public CommitTable.Client getClient() {
        return new Client();
    }

    public class Writer implements CommitTable.Writer {
        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) {
            // In this implementation, we use only one location that represents
            // both the value and the invalidation. Therefore, putIfAbsent is
            // required to make sure the entry was not invalidated.
            table.putIfAbsent(startTimestamp, commitTimestamp);
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            InMemoryCommitTable.this.lowWatermark = lowWatermark;
        }

        @Override
        public void flush() throws IOException {
            // noop
        }

        @Override
        public void clearWriteBuffer() {
            table.clear();
        }

        @Override
        public void close() {
        }
    }

    public class Client implements CommitTable.Client {
        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {
            SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
            Long result = table.get(startTimestamp);
            if (result == null) {
                f.set(Optional.<CommitTimestamp>absent());
            } else {
                if (result == INVALID_TRANSACTION_MARKER) {
                    f.set(Optional.of(new CommitTimestamp(Location.COMMIT_TABLE, INVALID_TRANSACTION_MARKER, false)));
                } else {
                    f.set(Optional.of(new CommitTimestamp(Location.COMMIT_TABLE, result, true)));
                }
            }
            return f;
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            SettableFuture<Long> f = SettableFuture.create();
            f.set(lowWatermark);
            return f;
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            SettableFuture<Void> f = SettableFuture.create();
            System.out.println("Not removing " + startTimestamp + "in memory commit table");
// for coprocessor side secondary index update            table.remove(startTimestamp);
            table.remove(startTimestamp);
            f.set(null);
            return f;	
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {

            SettableFuture<Boolean> f = SettableFuture.create();
            Long old = table.get(startTimestamp);

            // If the transaction represented by startTimestamp is not in the map
            if (old == null) {
                // Try to invalidate the transaction
                old = table.putIfAbsent(startTimestamp, INVALID_TRANSACTION_MARKER);
                // If we were able to invalidate or someone else invalidate before us
                if (old == null || old == INVALID_TRANSACTION_MARKER) {
                    f.set(true);
                    return f;
                }
            } else {
                // Check if the value we read marked the transaction as invalid
                if (old == INVALID_TRANSACTION_MARKER) {
                    f.set(true);
                    return f;
                }
            }

            // At this point the transaction was already in the map at the beginning
            // of the method or was added right before we tried to invalidate.
            f.set(false);
            return f;
        }

        @Override
        public void close() {
        }
    }

    public int countElements() {
        return table.size();
    }

}
