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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class OceanBaseWriter<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseWriter.class);

    private final TypeSerializer<T> serializer;
    private final OceanBaseWriterOptions writerOptions;
    private final StatementExecutor<T> statementExecutor;

    private final transient ScheduledExecutorService scheduler;
    private final transient ScheduledFuture<?> scheduledFuture;

    private transient int bufferCount = 0;
    private transient volatile Exception flushException = null;
    private transient volatile boolean closed = false;

    public OceanBaseWriter(
            OceanBaseWriterOptions writerOptions,
            TypeSerializer<T> serializer,
            StatementExecutor<T> statementExecutor) {
        this.writerOptions = writerOptions;
        this.serializer = serializer;
        this.statementExecutor = statementExecutor;

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
                        writerOptions.getBatchIntervalMs(),
                        writerOptions.getBatchIntervalMs(),
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void write(T record, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        T recordCopy = copyIfNecessary(record);
        statementExecutor.addToBatch(recordCopy);
        bufferCount++;
        if (bufferCount >= writerOptions.getBufferSize()) {
            flush(false);
        }
    }

    protected void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to OceanBase failed.", flushException);
        }
    }

    private T copyIfNecessary(T record) {
        return serializer == null ? record : serializer.copy(record);
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkFlushException();

        for (int i = 0; i <= writerOptions.getMaxRetries(); i++) {
            try {
                statementExecutor.executeBatch();
                bufferCount = 0;
                break;
            } catch (Exception e) {
                LOG.error("OceanBaseWriter flush error, retry times = {}", i, e);
                if (i >= writerOptions.getMaxRetries()) {
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
                if (statementExecutor != null) {
                    statementExecutor.close();
                }
            } catch (Exception e) {
                LOG.warn("Close statement executor failed", e);
            }
        }
        checkFlushException();
    }
}
