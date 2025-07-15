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

package com.oceanbase.connector.flink.sink.batch;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoadUtils;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

/** The direct load writer which supports multi node write. */
public class MultiNodeWriter extends ProcessFunction<RowData, Void> {

    private final String executionId;
    private final RecordSerializationSchema<RowData> recordSerializer;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final List<Record> buffer;
    private DirectLoader directLoader;

    public MultiNodeWriter(
            String executionId,
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer) {
        this.executionId = executionId;
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.buffer = Lists.newArrayListWithExpectedSize(connectorOptions.getBufferSize());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Preconditions.checkNotNull(this.executionId, "executionId must not be null");
        this.directLoader =
                DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions, executionId);
        this.directLoader.begin();
    }

    @Override
    public void processElement(
            RowData value, ProcessFunction<RowData, Void>.Context ctx, Collector<Void> out)
            throws Exception {
        Record record = recordSerializer.serialize(value);
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

    private void flush(List<Record> buffer) throws SQLException, ObDirectLoadException {
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

    @Override
    public void close() throws Exception {
        if (CollectionUtils.isNotEmpty(buffer)) {
            flush(buffer);
        }
        if (Objects.nonNull(directLoader)) {
            directLoader.close();
        }
    }
}
