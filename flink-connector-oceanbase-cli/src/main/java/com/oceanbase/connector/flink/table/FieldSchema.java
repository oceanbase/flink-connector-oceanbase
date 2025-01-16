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

import org.apache.flink.table.types.logical.LogicalType;

public class FieldSchema {
    private final String name;
    private final LogicalType type;
    private final String typeString;
    private final String defaultValue;
    private final String comment;
    private final Boolean nullable;

    public FieldSchema(
            String name,
            LogicalType type,
            String typeString,
            String defaultValue,
            String comment,
            Boolean nullable) {
        this.name = name;
        this.type = type;
        this.typeString = typeString;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public LogicalType getType() {
        return type;
    }

    public String getTypeString() {
        return typeString;
    }

    public String getComment() {
        return comment;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Boolean getNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='"
                + name
                + '\''
                + ", type="
                + type
                + ", typeString='"
                + typeString
                + '\''
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", comment='"
                + comment
                + '\''
                + ", nullable="
                + nullable
                + '}';
    }
}
