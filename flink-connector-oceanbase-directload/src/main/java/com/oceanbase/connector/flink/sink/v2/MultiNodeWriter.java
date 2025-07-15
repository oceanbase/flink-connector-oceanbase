/*
 * Copyright 2024 OceanBase.
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

package com.oceanbase.connector.flink.sink.v2;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

public class MultiNodeWriter
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<RowData, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MultiNodeWriter.class);
    private final RecordSerializationSchema<RowData> recordSerializer;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final List<Record> buffer;
    private final DirectLoader directLoader;

    public MultiNodeWriter(
            String executionId,
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.buffer = Lists.newArrayListWithExpectedSize(connectorOptions.getBufferSize());
        Preconditions.checkNotNull(executionId, "executionId must not be null");
        this.directLoader =
                DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, executionId);
        this.directLoader.begin();
    }

    @Override
    public Collection<String> prepareCommit() throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        Record record = recordSerializer.serialize(element);
        if (record == null) {
            return;
        }

        buffer.add(record);
        if (buffer.size() >= connectorOptions.getBufferSize()) {
            try {
                flush(buffer);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "The direct-load writer Failed to flash to table: %s.",
                                connectorOptions.getTableName()),
                        e);
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            LOG.info("Flush buffer size: {}", buffer.size());
            try {
                flush(buffer);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "The direct-load writer Failed to flash to table: %s.",
                                connectorOptions.getTableName()),
                        e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (CollectionUtils.isNotEmpty(buffer)) {
            flush(buffer);
        }
        if (Objects.nonNull(directLoader)) {
            directLoader.close();
        }
    }

    private void flush(List<Record> buffer) throws ObDirectLoadException {
        if (CollectionUtils.isEmpty(buffer)) {
            return;
        }
        ObDirectLoadBucket bucket = new ObDirectLoadBucket();
        for (Record record : buffer) {
            if (record instanceof DataChangeRecord) {
                DataChangeRecord dataChangeRecord = (DataChangeRecord) record;
                TableInfo table = (TableInfo) dataChangeRecord.getTable();
                List<String> fieldNames = table.getFieldNames();
                ObObj[] array = new ObObj[fieldNames.size()];
                int index = 0;
                for (String fieldName : fieldNames) {
                    array[index++] = createObObj(dataChangeRecord.getFieldValue(fieldName));
                }
                bucket.addRow(array);
            }
        }
        try {
            this.directLoader.write(bucket);
            buffer.clear();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "The direct-load writer Failed to flash to table: %s.",
                            connectorOptions.getTableName()),
                    e);
        }
    }
}
