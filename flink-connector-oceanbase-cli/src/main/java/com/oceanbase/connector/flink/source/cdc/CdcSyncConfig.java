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

package com.oceanbase.connector.flink.source.cdc;

public class CdcSyncConfig {

    /** Option key for cdc source. */
    public static final String SOURCE_CONF = "source-conf";

    /** Option key for oceanbase sink. */
    public static final String SINK_CONF = "sink-conf";

    // ------------------------------------------------------------
    // Source types
    // ------------------------------------------------------------
    public static final String MYSQL_CDC = "mysql-cdc";

    // ------------------------------------------------------------
    // Sync configurations
    // ------------------------------------------------------------
    public static final String JOB_NAME = "job-name";
    public static final String DATABASE = "database";
    public static final String TABLE_PREFIX = "table-prefix";
    public static final String TABLE_SUFFIX = "table-suffix";
    public static final String INCLUDING_TABLES = "including-tables";
    public static final String EXCLUDING_TABLES = "excluding-tables";
    public static final String MULTI_TO_ONE_ORIGIN = "multi-to-one-origin";
    public static final String MULTI_TO_ONE_TARGET = "multi-to-one-target";
    public static final String CREATE_TABLE_ONLY = "create-table-only";
    public static final String IGNORE_DEFAULT_VALUE = "ignore-default-value";
    public static final String IGNORE_INCOMPATIBLE = "ignore-incompatible";

    // ------------------------------------------------------------
    // Temporal configurations
    // ------------------------------------------------------------
    public static final String CONVERTERS = "converters";
    public static final String DATE = "date";
    public static final String DATE_TYPE = "date.type";
    public static final String DATE_FORMAT_DATE = "date.format.date";
    public static final String DATE_FORMAT_DATETIME = "date.format.datetime";
    public static final String DATE_FORMAT_TIMESTAMP = "date.format.timestamp";
    public static final String DATE_FORMAT_TIMESTAMP_ZONE = "date.format.timestamp.zone";
    public static final String YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATETIME_MICRO_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String TIME_ZONE_UTC_8 = "UTC+8";
    public static final String FORMAT_DATE = "format.date";
    public static final String FORMAT_TIME = "format.time";
    public static final String FORMAT_DATETIME = "format.datetime";
    public static final String FORMAT_TIMESTAMP = "format.timestamp";
    public static final String FORMAT_TIMESTAMP_ZONE = "format.timestamp.zone";
    public static final String UPPERCASE_DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
}
