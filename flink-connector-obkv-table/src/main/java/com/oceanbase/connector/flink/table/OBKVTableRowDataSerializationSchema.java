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

package com.oceanbase.connector.flink.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

public class OBKVTableRowDataSerializationSchema
        extends AbstractRecordSerializationSchema<RowData> {

    private final TableInfo tableInfo;

    public OBKVTableRowDataSerializationSchema(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    protected SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
        return null;
    }

    @Override
    public Record serialize(RowData data) {
        return null;
    }
}
