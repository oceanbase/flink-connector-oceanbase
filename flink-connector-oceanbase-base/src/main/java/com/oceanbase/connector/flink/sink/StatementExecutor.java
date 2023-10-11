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

import org.apache.flink.api.connector.sink2.Sink;

import java.io.Serializable;
import java.sql.SQLException;

public interface StatementExecutor<T> extends AutoCloseable, Serializable {

    /**
     * Set the sink context
     *
     * @param context a {@link Sink.InitContext} instance
     */
    void setContext(Sink.InitContext context);

    /**
     * Adds a record to batch
     *
     * @param record the row data record
     */
    void addToBatch(T record);

    /**
     * Submits a batch of records to OceanBase
     *
     * @throws SQLException if a database access error occurs
     */
    void executeBatch() throws Exception;
}
