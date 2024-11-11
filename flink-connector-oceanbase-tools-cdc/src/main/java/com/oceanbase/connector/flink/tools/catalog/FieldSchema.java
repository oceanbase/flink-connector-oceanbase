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

package com.oceanbase.connector.flink.tools.catalog;

public class FieldSchema {
    private String name;
    private String typeString;
    private String defaultValue;
    private String comment;

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    private Boolean nullable;

    public FieldSchema() {}

    public FieldSchema(String name, String typeString, String comment, Boolean nullable) {
        this.name = name;
        this.typeString = typeString;
        this.comment = comment;
        this.nullable = nullable;
    }

    public FieldSchema(
            String name, String typeString, String defaultValue, String comment, Boolean nullable) {
        this.name = name;
        this.typeString = typeString;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeString() {
        return typeString;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='"
                + name
                + '\''
                + ", typeString='"
                + typeString
                + '\''
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", comment='"
                + comment
                + '\''
                + ", nullable='"
                + nullable
                + '\''
                + '}';
    }
}
