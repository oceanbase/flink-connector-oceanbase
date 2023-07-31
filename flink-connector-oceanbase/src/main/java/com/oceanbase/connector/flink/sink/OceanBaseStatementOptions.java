/*
 * Copyright (c) 2023 OceanBase
 * flink-connector-oceanbase is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
