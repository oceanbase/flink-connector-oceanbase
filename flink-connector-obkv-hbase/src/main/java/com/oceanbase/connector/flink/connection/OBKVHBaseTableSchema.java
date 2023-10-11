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
