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

import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.partition.calculator.model.TableEntryKey;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;

public class OceanBaseConnectionInfo {

    public enum CompatibleMode {
        MYSQL,
        ORACLE;

        public static CompatibleMode fromString(@Nonnull String text) {
            switch (text.trim().toUpperCase()) {
                case "MYSQL":
                    return MYSQL;
                case "ORACLE":
                    return ORACLE;
                default:
                    throw new UnsupportedOperationException("Unsupported compatible mode: " + text);
            }
        }

        public boolean isMySqlMode() {
            return CompatibleMode.MYSQL.equals(this);
        }
    }

    public enum Version {
        LEGACY,
        V4;

        public static Version fromString(@Nonnull String text) {
            return (StringUtils.isBlank(text) || !text.startsWith("4.")) ? LEGACY : V4;
        }

        public boolean isV4() {
            return Version.V4.equals(this);
        }
    }

    private final OceanBaseDialect dialect;
    private final Version version;
    private final TableEntryKey tableEntryKey;

    public OceanBaseConnectionInfo(
            OceanBaseDialect dialect, Version version, TableEntryKey tableEntryKey) {
        this.dialect = dialect;
        this.version = version;
        this.tableEntryKey = tableEntryKey;
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    public Version getVersion() {
        return version;
    }

    public TableEntryKey getTableEntryKey() {
        return tableEntryKey;
    }
}
