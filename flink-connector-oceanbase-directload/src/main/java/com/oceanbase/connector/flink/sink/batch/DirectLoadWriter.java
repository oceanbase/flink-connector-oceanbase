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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

/** The direct-load sink writer. see {@link SinkWriter}. */
public class DirectLoadWriter implements SinkWriter<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(DirectLoadWriter.class);
    private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();

    private final RecordSerializationSchema<RowData> recordSerializer;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final DirectLoader directLoader;
    private final Map<Thread, List<Record>> bufferMap = Maps.newConcurrentMap();
    private final ExecutorService executorService;

    public DirectLoadWriter(
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<RowData> recordSerializer,
            int numberOfTaskSlots) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.executorService =
                new ThreadPoolExecutor(
                        Math.min(numberOfTaskSlots, CORE_NUM),
                        Math.min(numberOfTaskSlots, CORE_NUM) * 2,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        Executors.defaultThreadFactory(),
                        new ThreadPoolExecutor.CallerRunsPolicy());

        this.directLoader = DirectLoadUtils.buildDirectLoaderFromConnOption(connectorOptions);
        try {
            directLoader.begin();
            LOG.info("The direct-load writer begin successfully.");
        } catch (SQLException e) {
            throw new RuntimeException("The direct-load writer begin failed.", e);
        }
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        Record record = recordSerializer.serialize(element);
        if (record == null) {
            return;
        }

        executorService.execute(
                () -> {
                    List<Record> buffer =
                            bufferMap.computeIfAbsent(
                                    Thread.currentThread(),
                                    recordBuffer ->
                                            Lists.newArrayListWithExpectedSize(
                                                    connectorOptions.getBufferSize()));
                    buffer.add(record);
                    if (buffer.size() >= connectorOptions.getBufferSize()) {
                        try {
                            flush(buffer);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to flash to OceanBase.", e);
                        }
                    }
                });
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
            directLoader.write(bucket);
            buffer.clear();
        } catch (Exception e) {
            throw new SQLException("Failed to flash to OceanBase.", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            executorService.shutdown();
            boolean termination = executorService.awaitTermination(600, TimeUnit.SECONDS);
            try {
                if (termination) {
                    for (List<Record> buffer : bufferMap.values()) {
                        flush(buffer);
                    }
                    if (!connectorOptions.getEnableMultiNodeWrite()) {
                        directLoader.commit();
                    }
                    LOG.info("The direct-load write to oceanbase succeed.");
                } else {
                    throw new RuntimeException(
                            "The direct-load write to oceanbase timeout after 600 seconds.");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to write to OceanBase.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(directLoader)) {
            directLoader.close();
        }
    }
}
