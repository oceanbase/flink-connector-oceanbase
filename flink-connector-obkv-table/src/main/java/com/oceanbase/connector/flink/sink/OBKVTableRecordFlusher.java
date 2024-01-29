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

import com.oceanbase.connector.flink.OBKVTableConnectorOptions;
import com.oceanbase.connector.flink.connection.OBKVTableConnectionProvider;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.SchemaChangeRecord;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;

import java.util.List;

public class OBKVTableRecordFlusher implements RecordFlusher {

    private final OBKVTableConnectorOptions options;
    private final OBKVTableConnectionProvider connectionProvider;

    public OBKVTableRecordFlusher(OBKVTableConnectorOptions options) {
        this(options, new OBKVTableConnectionProvider(options));
    }

    public OBKVTableRecordFlusher(
            OBKVTableConnectorOptions options, OBKVTableConnectionProvider connectionProvider) {
        this.options = options;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void flush(SchemaChangeRecord record) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush(List<DataChangeRecord> records) throws Exception {
        if (records == null || records.isEmpty()) {
            return;
        }

        TableInfo tableInfo = (TableInfo) records.get(0).getTable();
        TableId tableId = tableInfo.getTableId();
    }

    @Override
    public void close() throws Exception {}
}
