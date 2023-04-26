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

import org.apache.commons.lang3.StringUtils;

public enum OceanBaseCompatibleMode {
    MYSQL,
    ORACLE;

    public static OceanBaseCompatibleMode fromString(String text) {
        if (StringUtils.isBlank(text)) {
            throw new IllegalArgumentException("Compatible mode should not be blank");
        }
        switch (text.trim().toUpperCase()) {
            case "MYSQL":
                return MYSQL;
            case "ORACLE":
                return ORACLE;
            default:
                throw new UnsupportedOperationException("Unsupported compatible mode: " + text);
        }
    }
}
