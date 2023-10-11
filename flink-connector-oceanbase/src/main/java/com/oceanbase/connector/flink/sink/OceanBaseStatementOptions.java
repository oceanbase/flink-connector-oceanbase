/*
 * Copyright (c) 2023 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink.sink;

import java.io.Serializable;

public class OceanBaseStatementOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean upsertMode;
    private final int batchSize;

    private final boolean memStoreCheckEnabled;
    private final double memStoreThreshold;
    private final long memStoreCheckInterval;
    private final boolean partitionEnabled;
    private final int partitionNumber;

    public OceanBaseStatementOptions(
            boolean upsertMode,
            int batchSize,
            boolean memStoreCheckEnabled,
            double memStoreThreshold,
            long memStoreCheckInterval,
            boolean partitionEnabled,
            int partitionNumber) {
        this.upsertMode = upsertMode;
        this.batchSize = batchSize;
        this.memStoreCheckEnabled = memStoreCheckEnabled;
        this.memStoreThreshold = memStoreThreshold;
        this.memStoreCheckInterval = memStoreCheckInterval;
        this.partitionEnabled = partitionEnabled;
        this.partitionNumber = partitionNumber;
    }

    public boolean isUpsertMode() {
        return upsertMode;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isMemStoreCheckEnabled() {
        return memStoreCheckEnabled;
    }

    public double getMemStoreThreshold() {
        return memStoreThreshold;
    }

    public long getMemStoreCheckInterval() {
        return memStoreCheckInterval;
    }

    public boolean isPartitionEnabled() {
        return partitionEnabled;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }
}
