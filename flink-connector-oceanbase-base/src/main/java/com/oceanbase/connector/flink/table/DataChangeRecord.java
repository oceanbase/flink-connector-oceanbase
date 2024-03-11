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

package com.oceanbase.connector.flink.table;

import java.io.Serializable;
import java.util.Optional;

public class DataChangeRecord implements Record {

    private static final long serialVersionUID = 1L;

    public enum Type {
        UPSERT,
        DELETE,
    }

    public interface KeyExtractor extends Serializable {

        Object extract(DataChangeRecord record);

        static KeyExtractor simple() {
            return record ->
                    Optional.ofNullable(record.getTable().getKey())
                            .map(
                                    keys ->
                                            new DataChangeRecordData(
                                                    keys.stream()
                                                            .map(record::getFieldValue)
                                                            .toArray()))
                            .orElse(null);
        }
    }

    private final Table table;
    private final Type type;
    private final DataChangeRecordData data;

    public DataChangeRecord(Table table, Type type, Object[] values) {
        this.table = table;
        this.type = type;
        this.data = new DataChangeRecordData(values);
    }

    @Override
    public TableId getTableId() {
        return table.getTableId();
    }

    public Table getTable() {
        return table;
    }

    public Type getType() {
        return type;
    }

    public boolean isUpsert() {
        return Type.UPSERT == getType();
    }

    public Object getFieldValue(String fieldName) {
        return data.getValue(table.getFieldIndex(fieldName));
    }

    @Override
    public String toString() {
        return "DataChangeRecord{" + "table=" + table + ", type=" + type + ", data=" + data + '}';
    }
}
