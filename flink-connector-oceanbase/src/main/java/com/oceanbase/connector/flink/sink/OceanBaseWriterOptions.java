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

public class OceanBaseWriterOptions implements Serializable {

    private final String tableName;
    private final boolean upsertMode;

    private final long batchIntervalMs;
    private final int bufferSize;
    private final int batchSize;
    private final int maxRetries;
    private final boolean memStoreCheckEnabled;
    private final double memStoreThreshold;
    private final long memStoreCheckInterval;

    public OceanBaseWriterOptions(
            String tableName,
            boolean upsertMode,
            long batchIntervalMs,
            int bufferSize,
            int batchSize,
            int maxRetries,
            boolean memStoreCheckEnabled,
            double memStoreThreshold,
            long memStoreCheckInterval) {
        this.tableName = tableName;
        this.upsertMode = upsertMode;
        this.batchIntervalMs = batchIntervalMs;
        this.bufferSize = bufferSize;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.memStoreCheckEnabled = memStoreCheckEnabled;
        this.memStoreThreshold = memStoreThreshold;
        this.memStoreCheckInterval = memStoreCheckInterval;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isUpsertMode() {
        return upsertMode;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
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
}
