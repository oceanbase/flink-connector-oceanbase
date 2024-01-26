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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class OceanBaseWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseWriter.class);

    private final ConnectorOptions options;
    private final SinkWriterMetricGroup metricGroup;
    private final TypeSerializer<T> typeSerializer;
    private final RecordSerializationSchema<T> recordSerializer;
    private final DataChangeRecord.KeyExtractor keyExtractor;
    private final RecordFlusher recordFlusher;

    private final AtomicReference<Record> currentRecord = new AtomicReference<>();

    private final Map<String, List<DataChangeRecord>> buffer = new HashMap<>();
    private final Map<String, Map<Object, DataChangeRecord>> reducedBuffer = new HashMap<>();

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
            DataChangeRecord.KeyExtractor keyExtractor,
            RecordFlusher recordFlusher) {
        this.options = options;
        this.metricGroup = initContext.metricGroup();
        this.typeSerializer = typeSerializer;
        this.recordSerializer = recordSerializer;
        this.keyExtractor = keyExtractor;
        this.recordFlusher = recordFlusher;
        this.scheduler =
                (options.getSyncWrite() || options.getBufferFlushInterval() == 0)
                        ? null
                        : new ScheduledThreadPoolExecutor(
                                1, new ExecutorThreadFactory("OceanBaseWriter.scheduler"));
        this.scheduledFuture =
                scheduler == null
                        ? null
                        : this.scheduler.scheduleWithFixedDelay(
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

        if (!options.getSyncWrite() && keyExtractor == null) {
            throw new IllegalArgumentException(
                    "When 'sync-write' is not enabled, keyExtractor is required to construct the buffer key.");
        }
    }

    @Override
    public synchronized void write(T data, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        T copy = copyIfNecessary(data);
        Record record = recordSerializer.serialize(copy);
        if (record == null) {
            return;
        }
        if (!(record instanceof SchemaChangeRecord) && !(record instanceof DataChangeRecord)) {
            LOG.info("Discard unsupported record: {}", record);
            return;
        }

        if (options.getSyncWrite() || record instanceof SchemaChangeRecord) {
            // redundant check, currentRecord should always be null here
            while (!currentRecord.compareAndSet(null, record)) {
                flush(false);
            }
            metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
            flush(false);
        } else {
            DataChangeRecord dataChangeRecord = (DataChangeRecord) record;
            Object key = keyExtractor.extract(dataChangeRecord);
            if (key == null) {
                synchronized (buffer) {
                    buffer.computeIfAbsent(record.getTableId().identifier(), k -> new ArrayList<>())
                            .add(dataChangeRecord);
                }
            } else {
                synchronized (reducedBuffer) {
                    reducedBuffer
                            .computeIfAbsent(record.getTableId().identifier(), k -> new HashMap<>())
                            .put(key, dataChangeRecord);
                }
            }
            bufferCount++;
            metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
            if (bufferCount >= options.getBufferSize()) {
                flush(false);
            }
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
                // async write buffer
                if (!buffer.isEmpty()) {
                    synchronized (buffer) {
                        for (Map.Entry<String, List<DataChangeRecord>> entry : buffer.entrySet()) {
                            List<DataChangeRecord> recordList = entry.getValue();
                            recordFlusher.flush(recordList);
                            metricGroup
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .inc(recordList.size());
                        }
                        buffer.clear();
                    }
                }
                if (!reducedBuffer.isEmpty()) {
                    synchronized (reducedBuffer) {
                        for (Map.Entry<String, Map<Object, DataChangeRecord>> entry :
                                reducedBuffer.entrySet()) {
                            Map<Object, DataChangeRecord> recordMap = entry.getValue();
                            recordFlusher.flush(new ArrayList<>(recordMap.values()));
                            metricGroup
                                    .getIOMetricGroup()
                                    .getNumRecordsOutCounter()
                                    .inc(recordMap.size());
                        }
                        reducedBuffer.clear();
                    }
                }

                // sync write current record
                Record record = currentRecord.get();
                if (record == null) {
                    return;
                }
                if (record instanceof SchemaChangeRecord) {
                    recordFlusher.flush((SchemaChangeRecord) record);
                } else {
                    recordFlusher.flush(Collections.singletonList((DataChangeRecord) record));
                }
                metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc();
                currentRecord.compareAndSet(record, null);
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
