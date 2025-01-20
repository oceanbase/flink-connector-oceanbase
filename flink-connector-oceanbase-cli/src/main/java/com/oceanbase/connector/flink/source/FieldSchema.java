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

package com.oceanbase.connector.flink.source;

/** The schema information of table fields. */
public class FieldSchema {
    private final String name;
    private final FieldType type;
    private final String defaultValue;
    private final String comment;
    private final boolean nullable;

    public FieldSchema(
            String name, FieldType type, String defaultValue, String comment, boolean nullable) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public FieldType getType() {
        return type;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getComment() {
        return comment;
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
