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

package com.oceanbase.connector.flink.directload;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.execution.ObDirectLoadStatementExecutionId;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.ObVString;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** Wrapper of the direct-load api. */
public class DirectLoader {
    private static final Logger LOG = LoggerFactory.getLogger(DirectLoader.class);

    private final DirectLoaderBuilder builder;
    private final String schemaTableName;
    private final ObDirectLoadStatement statement;
    private final ObDirectLoadConnection connection;

    private String executionId;

    public DirectLoader(
            DirectLoaderBuilder builder,
            String schemaTableName,
            ObDirectLoadStatement statement,
            ObDirectLoadConnection connection) {
        this.builder = builder;
        this.schemaTableName = schemaTableName;
        this.statement = statement;
        this.connection = connection;
    }

    public DirectLoader(
            DirectLoaderBuilder builder,
            String schemaTableName,
            ObDirectLoadStatement statement,
            ObDirectLoadConnection connection,
            String executionId) {
        this(builder, schemaTableName, statement, connection);
        this.executionId = executionId;
    }

    public String begin() throws SQLException {
        try {
            LOG.info("{} direct load beginning ......", schemaTableName);
            if (Objects.isNull(executionId)) {
                statement.begin();
                ObDirectLoadStatementExecutionId statementExecutionId = statement.getExecutionId();
                byte[] executionIdBytes = statementExecutionId.encode();
                this.executionId = Base64.getEncoder().encodeToString(executionIdBytes);
                LOG.info("{} direct load execution id : {}", schemaTableName, this.executionId);
                builder.executionId(executionId);
            } else {
                ObDirectLoadStatementExecutionId statementExecutionId =
                        new ObDirectLoadStatementExecutionId();
                byte[] executionIdBytes = Base64.getDecoder().decode(executionId);
                statementExecutionId.decode(executionIdBytes);
                statement.resume(statementExecutionId);
                LOG.info(
                        "{} direct load resume from execution id : {} success",
                        schemaTableName,
                        this.executionId);
            }
            return executionId;
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
    }

    public void write(ObDirectLoadBucket bucket) throws SQLException {
        try {
            this.statement.write(bucket);
        } catch (Exception e) {
            throw new SQLException(
                    String.format(
                            "Failed to write to table: %s, execution id: %s",
                            schemaTableName, executionId),
                    e);
        }
    }

    public void commit() throws SQLException {
        try {
            statement.commit();
        } catch (Exception e) {
            throw new SQLException(
                    String.format(
                            "Failed to commit, table: %s, execution id: %s",
                            schemaTableName, executionId),
                    e);
        }
    }

    public void close() {
        if (Objects.nonNull(statement)) {
            this.statement.close();
        }
        if (Objects.nonNull(connection)) {
            this.connection.close();
        }
    }

    public DirectLoaderBuilder getBuilder() {
        return builder;
    }

    public String getSchemaTableName() {
        return schemaTableName;
    }

    public ObDirectLoadStatement getStatement() {
        return statement;
    }

    public static ObObj[] createObObjArray(List<?> values) {
        if (values == null) {
            return null;
        }
        ObObj[] array = new ObObj[values.size()];
        for (int i = 0; i < values.size(); i++) {
            array[i] = createObObj(values.get(i));
        }
        return array;
    }

    public static ObObj createObObj(Object value) {
        try {
            // Only used for strongly typed declared variables
            Object convertedValue = value == null ? null : convertValue(value);
            return new ObObj(ObObjType.defaultObjMeta(convertedValue), convertedValue);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Some values with data type is unsupported by ObObjType#valueOfType. We should convert the
     * input value to supported value data type.
     */
    public static Object convertValue(Object pairValue) throws Exception {
        if (pairValue == null) {
            return null;
        }
        Object value = pairValue;

        if (value instanceof BigDecimal) {
            return value.toString();
        } else if (value instanceof BigInteger) {
            return value.toString();
        } else if (value instanceof Instant) {
            return Timestamp.from(((Instant) value));
        } else if (value instanceof LocalDate) {
            LocalDateTime ldt = ((LocalDate) value).atTime(0, 0);
            return Timestamp.valueOf(ldt);
        } else if (value instanceof LocalTime) {
            // Warn: java.sql.Time.valueOf() is deprecated.
            Time t = Time.valueOf((LocalTime) value);
            return new Timestamp(t.getTime());
        } else if (value instanceof LocalDateTime) {
            return Timestamp.valueOf(((LocalDateTime) value));
        } else if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        } else if (value instanceof Time) {
            return new Timestamp(((Time) value).getTime());
        } else if (value instanceof ZonedDateTime) {
            // Note: Be care of time zone!!!
            return Timestamp.from(((ZonedDateTime) value).toInstant());
        } else if (value instanceof OffsetTime) {
            LocalTime lt = ((OffsetTime) value).toLocalTime();
            // Warn: java.sql.Time.valueOf() is deprecated.
            return new Timestamp(Time.valueOf(lt).getTime());
        } else if (value instanceof InputStream) {
            try (InputStream is = ((InputStream) value)) {
                // Note: Be care of character set!!!
                return new ObVString(IOUtils.toString(is, Charset.defaultCharset()));
            }
        } else if (value instanceof Blob) {
            Blob b = (Blob) value;
            try (InputStream is = b.getBinaryStream()) {
                // Note: Be care of character set!!!
                if (is == null) {
                    return null;
                }
                return new ObVString(IOUtils.toString(is, Charset.defaultCharset()));
            } finally {
                b.free();
            }
        } else if (value instanceof Reader) {
            try (Reader r = ((Reader) value)) {
                return IOUtils.toString(r);
            }
        } else if (value instanceof Clob) {
            Clob c = (Clob) value;
            try (Reader r = c.getCharacterStream()) {
                return r == null ? null : IOUtils.toString(r);
            } finally {
                c.free();
            }
        } else {
            return value;
        }
    }
}
