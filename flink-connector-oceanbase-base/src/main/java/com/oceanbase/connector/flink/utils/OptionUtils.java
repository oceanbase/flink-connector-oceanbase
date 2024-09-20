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

package com.oceanbase.connector.flink.utils;

import org.apache.flink.configuration.ConfigurationUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class OptionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OptionUtils.class);

    /** Utility class can not be instantiated. */
    private OptionUtils() {}

    public static void printOptions(String identifier, Map<String, String> config) {
        Map<String, String> hideMap = ConfigurationUtils.hideSensitiveValues(config);
        LOG.info("Print {} connector configuration:", identifier);
        for (String key : hideMap.keySet()) {
            LOG.info("{} = {}", key, hideMap.get(key));
        }
    }

    public static Properties parseProperties(String propsStr) {
        if (StringUtils.isBlank(propsStr)) {
            return null;
        }
        Properties props = new Properties();
        for (String propStr : propsStr.split(";")) {
            if (StringUtils.isBlank(propStr)) {
                continue;
            }
            String[] pair = propStr.trim().split("=");
            if (pair.length != 2) {
                throw new IllegalArgumentException("properties must have one key value pair");
            }
            props.put(pair[0].trim(), pair[1].trim());
        }
        return props;
    }
}
