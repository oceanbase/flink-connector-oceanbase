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

import com.oceanbase.connector.flink.ConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.SerializableFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class OceanBaseWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseWriter.class);

    private final ConnectorOptions options;
    private final SinkWriterMetricGroup metricGroup;
    private final TypeSerializer<T> typeSerializer;
    private final RecordSerializationSchema<T> recordSerializer;
    private final SerializableFunction<DataChangeRecord, String> keyExtractor;
    private final RecordFlusher recordFlusher;

    private final List<SchemaChangeRecord> schemaChangeRecords = new ArrayList<>(1);
    private final Map<String, List<DataChangeRecord>> dataChangeRecordBuffer = new HashMap<>();
    private final Map<String, Map<String, DataChangeRecord>> dataChangeRecordReducedBuffer =
            new HashMap<>();

    private final transient ScheduledExecutorService scheduler;
    private final transient ScheduledFuture<?> scheduledFuture;

    private transient int bufferCount = 0;
    private transient volatile Exception flushException = null;
    private transient volatile boolean closed = false;

    public OceanBaseWriter(
            ConnectorOptions options,
            Sink.InitContext initContext,
            TypeSerializer<T> typeSerializer,
            RecordSerializationSchema<T> recordSerializer,
            SerializableFunction<DataChangeRecord, String> keyExtractor,
            RecordFlusher recordFlusher) {
        this.options = options;
        this.metricGroup = initContext.metricGroup();
        this.typeSerializer = typeSerializer;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.recordFlusher = recordFlusher;
        this.scheduler =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("OceanBaseWriter.scheduler"));
        this.scheduledFuture =
                this.scheduler.scheduleWithFixedDelay(
                        () -> {
                            if (!closed) {
                                try {
                                    synchronized (this) {
                                        flush(false);
                                    }
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        },
                        options.getBufferFlushInterval(),
                        options.getBufferFlushInterval(),
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void write(T data, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        T copy = copyIfNecessary(data);
        Record record = recordSerializer.serialize(copy);
        addToBuffer(record);
        if (record instanceof SchemaChangeRecord || bufferCount >= options.getBufferSize()) {
            flush(false);
        }
    }

    private void addToBuffer(Record record) {
        if (record instanceof SchemaChangeRecord) {
            synchronized (schemaChangeRecords) {
                schemaChangeRecords.add((SchemaChangeRecord) record);
                bufferCount++;
                metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
            }
        } else if (record instanceof DataChangeRecord) {
            DataChangeRecord dataChangeRecord = (DataChangeRecord) record;
            String key = keyExtractor == null ? null : keyExtractor.apply(dataChangeRecord);
            if (key == null) {
                synchronized (dataChangeRecordBuffer) {
                    dataChangeRecordBuffer
                            .computeIfAbsent(dataChangeRecord.getTableId(), k -> new ArrayList<>())
                            .add(dataChangeRecord);
                    bufferCount++;
                    metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
                }
            } else {
                synchronized (dataChangeRecordReducedBuffer) {
                    dataChangeRecordReducedBuffer
                            .computeIfAbsent(dataChangeRecord.getTableId(), k -> new HashMap<>())
                            .put(key, dataChangeRecord);
                    bufferCount++;
                    metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
                }
            }
        } else {
            throw new IllegalArgumentException(
                    "Unsupported record, class: "
                            + record.getClass().getCanonicalName()
                            + ", data: "
                            + record);
        }
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to OceanBase failed.", flushException);
        }
    }

    private T copyIfNecessary(T record) {
        return typeSerializer == null ? record : typeSerializer.copy(record);
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkFlushException();

        for (int i = 0; i <= options.getMaxRetries(); i++) {
            try {
                if (!dataChangeRecordBuffer.isEmpty()) {
                    synchronized (dataChangeRecordBuffer) {
                        for (Map.Entry<String, List<DataChangeRecord>> entry :
                                dataChangeRecordBuffer.entrySet()) {
                            List<DataChangeRecord> recordList = entry.getValue();
                            recordFlusher.flush(recordList);
                            metricGroup
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .inc(recordList.size());
                        }
                        dataChangeRecordBuffer.clear();
                    }
                }
                if (!dataChangeRecordReducedBuffer.isEmpty()) {
                    synchronized (dataChangeRecordReducedBuffer) {
                        for (Map.Entry<String, Map<String, DataChangeRecord>> entry :
                                dataChangeRecordReducedBuffer.entrySet()) {
                            Map<String, DataChangeRecord> recordMap = entry.getValue();
                            recordFlusher.flush(new ArrayList<>(recordMap.values()));
                            metricGroup
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .inc(recordMap.size());
                        }
                        dataChangeRecordReducedBuffer.clear();
                    }
                }
                if (!schemaChangeRecords.isEmpty()) {
                    synchronized (schemaChangeRecords) {
                        for (SchemaChangeRecord record : schemaChangeRecords) {
                            recordFlusher.flush(record);
                        }
                        metricGroup
                                .getIOMetricGroup()
                                .getNumRecordsOutCounter()
                                .inc(schemaChangeRecords.size());
                        schemaChangeRecords.clear();
                    }
                }

                bufferCount = 0;
                break;
            } catch (Exception e) {
                LOG.error("OceanBaseWriter flush error, retry times = {}", i, e);
                if (i >= options.getMaxRetries()) {
                    throw new IOException(e);
                }
                Thread.sleep(1000L * i);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }

            if (bufferCount > 0) {
                try {
                    flush(true);
                } catch (Exception e) {
                    LOG.warn("Writing records to OceanBase failed", e);
                    throw new RuntimeException("Writing records to OceanBase failed", e);
                }
            }

            try {
                if (recordFlusher != null) {
                    recordFlusher.close();
                }
            } catch (Exception e) {
                LOG.warn("Close statement executor failed", e);
            }
        }
        checkFlushException();
    }
}
