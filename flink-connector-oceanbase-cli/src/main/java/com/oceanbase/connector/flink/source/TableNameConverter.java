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

import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TableNameConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String prefix;
    private final String suffix;
    private final Map<Pattern, String> multiToOneRulesPattern;

    public TableNameConverter(
            String prefix, String suffix, String multiToOneOrigin, String multiToOneTarget) {
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
        this.multiToOneRulesPattern = multiToOneRulesParser(multiToOneOrigin, multiToOneTarget);
    }

    public static HashMap<Pattern, String> multiToOneRulesParser(
            String multiToOneOrigin, String multiToOneTarget) {
        if (StringUtils.isNullOrWhitespaceOnly(multiToOneOrigin)
                || StringUtils.isNullOrWhitespaceOnly(multiToOneTarget)) {
            return null;
        }
        HashMap<Pattern, String> multiToOneRulesPattern = new HashMap<>();
        String[] origins = multiToOneOrigin.split("\\|");
        String[] targets = multiToOneTarget.split("\\|");
        if (origins.length != targets.length) {
            throw new IllegalArgumentException(
                    "The lengths of multiToOneOrigin and multiToOneTarget are not equal");
        }
        try {
            for (int i = 0; i < origins.length; i++) {
                multiToOneRulesPattern.put(Pattern.compile(origins[i]), targets[i]);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse multiToOnePattern", e);
        }
        return multiToOneRulesPattern;
    }

    public String convert(String tableName) {
        if (multiToOneRulesPattern == null) {
            return prefix + tableName + suffix;
        }

        String target = null;

        for (Map.Entry<Pattern, String> entry : multiToOneRulesPattern.entrySet()) {
            if (entry.getKey().matcher(tableName).matches()) {
                target = entry.getValue();
            }
        }

        if (target == null) {
            return prefix + tableName + suffix;
        }
        return target;
    }
}
