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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
    private String database;
    private String table;
    private String tableComment;
    private Map<String, FieldSchema> fields;
    private List<String> keys = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getTableComment() {
        return tableComment;
    }

    public Map<String, FieldSchema> getFields() {
        return fields;
    }

    public List<String> getKeys() {
        return keys;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public void setFields(Map<String, FieldSchema> fields) {
        this.fields = fields;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "TableSchema{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", tableComment='"
                + tableComment
                + '\''
                + ", fields="
                + fields
                + ", keys="
                + String.join(",", keys)
                + ", properties="
                + properties
                + '}';
    }
}
