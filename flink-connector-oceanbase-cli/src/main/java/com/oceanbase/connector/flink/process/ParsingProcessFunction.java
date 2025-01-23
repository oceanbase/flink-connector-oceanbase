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
package com.oceanbase.connector.flink.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class ParsingProcessFunction extends ProcessFunction<String, Void> {

    private final TableNameConverter converter;
    private final ObjectMapper objectMapper;

    private transient Map<String, OutputTag<String>> recordOutputTags;

    public ParsingProcessFunction(TableNameConverter converter) {
        this.converter = converter;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        recordOutputTags = new HashMap<>();
    }

    @Override
    public void processElement(
            String record, ProcessFunction<String, Void>.Context context, Collector<Void> collector)
            throws Exception {
        String tableName = getRecordTableName(record);
        String oceanbaseName = converter.convert(tableName);
        context.output(getRecordOutputTag(oceanbaseName), record);
    }

    protected String getRecordTableName(String record) throws Exception {
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        return extractJsonNode(recordRoot.get("source"), "table");
    }

    protected String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private OutputTag<String> getRecordOutputTag(String tableName) {
        return recordOutputTags.computeIfAbsent(
                tableName, ParsingProcessFunction::createRecordOutputTag);
    }

    public static OutputTag<String> createRecordOutputTag(String tableName) {
        return new OutputTag<String>("record-" + tableName) {};
    }
}
