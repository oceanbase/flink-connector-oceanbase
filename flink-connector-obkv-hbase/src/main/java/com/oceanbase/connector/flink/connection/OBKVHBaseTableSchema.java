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

package com.oceanbase.connector.flink.connection;

import com.oceanbase.connector.flink.converter.OBKVHBaseRowConverter;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OBKVHBaseTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private RowData.FieldGetter rowKeyFieldGetter;
    private final Map<String, Map<String, RowData.FieldGetter>> familyQualifierFieldGetterMap =
            new HashMap<>();
    private final Map<String, Integer> familyIndexMap = new HashMap<>();

    public OBKVHBaseTableSchema(ResolvedSchema resolvedSchema) {
        OBKVHBaseRowConverter converter = new OBKVHBaseRowConverter();
        int fieldIndex = 0;
        for (Column column : resolvedSchema.getColumns()) {
            LogicalType fieldType = column.getDataType().getLogicalType();
            if (fieldType.getTypeRoot() == LogicalTypeRoot.ROW) {
                RowType familyType = (RowType) fieldType;
                String familyName = column.getName();
                familyIndexMap.put(familyName, fieldIndex);
                Map<String, RowData.FieldGetter> qualifierFieldGetterMap = new HashMap<>();
                int qualifierIndex = 0;
                for (RowType.RowField qualifier : familyType.getFields()) {
                    qualifierFieldGetterMap.put(
                            qualifier.getName(),
                            converter.createFieldGetter(qualifier.getType(), qualifierIndex++));
                }
                this.familyQualifierFieldGetterMap.put(familyName, qualifierFieldGetterMap);
            } else if (fieldType.getChildren().isEmpty()) {
                if (this.rowKeyFieldGetter != null) {
                    throw new IllegalArgumentException("Row key can't be set multiple times");
                }
                this.rowKeyFieldGetter = converter.createFieldGetter(fieldType, fieldIndex);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported field type '" + fieldType + "' for HBase.");
            }
            fieldIndex++;
        }
    }

    public RowData.FieldGetter getRowKeyFieldGetter() {
        return rowKeyFieldGetter;
    }

    public Map<String, Map<String, RowData.FieldGetter>> getFamilyQualifierFieldGetterMap() {
        return familyQualifierFieldGetterMap;
    }

    public Map<String, Integer> getFamilyIndexMap() {
        return familyIndexMap;
    }
}
