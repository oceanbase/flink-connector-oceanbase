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

package com.oceanbase.connector.flink.converter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.Serializable;

public abstract class AbstractRowConverter implements Serializable {

    public RowData.FieldGetter createFieldGetter(LogicalType type, int fieldIndex) {
        return row -> createNullableExternalConverter(type).toExternal(row, fieldIndex);
    }

    public interface FieldConverter extends Serializable {
        Object toExternal(RowData rowData, int fieldIndex);
    }

    public FieldConverter createNullableExternalConverter(LogicalType type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }

    protected abstract FieldConverter createExternalConverter(LogicalType type);

    protected FieldConverter wrapIntoNullableExternalConverter(
            FieldConverter fieldConverter, LogicalType type) {
        return (val, fieldIndex) -> {
            if (val == null
                    || val.isNullAt(fieldIndex)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                return null;
            } else {
                return fieldConverter.toExternal(val, fieldIndex);
            }
        };
    }
}
