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

public class TransactionRecord implements Record {

    private static final long serialVersionUID = 1L;

    public enum Type {
        BEGIN,
        COMMIT,
        ROLLBACK
    }

    private final TableId tableId;
    private final Type type;

    public TransactionRecord(TableId tableId, Type type) {
        this.tableId = tableId;
        this.type = type;
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    public Type getType() {
        return type;
    }
}
