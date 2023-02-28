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
import java.sql.SQLException;

public interface OceanBaseStatementExecutor<T> extends AutoCloseable, Serializable {

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
    void executeBatch() throws SQLException;
}
