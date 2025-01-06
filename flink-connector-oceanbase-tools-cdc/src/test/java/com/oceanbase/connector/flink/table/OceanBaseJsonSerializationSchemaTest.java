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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OceanBaseJsonSerializationSchemaTest {

    @Test
    void testCreateNotNullConverter() {
        OceanBaseJsonSerializationSchemaConverterTest schema =
                new OceanBaseJsonSerializationSchemaConverterTest(null);
        // Test Boolean
        assertEquals(true, schema.createNotNullConverter(new BooleanType()).convert(true));

        // Test TinyInt
        assertEquals(
                (byte) 123, schema.createNotNullConverter(new TinyIntType()).convert((byte) 123));

        // Test SmallInt
        assertEquals(
                (short) 12345,
                schema.createNotNullConverter(new SmallIntType()).convert((short) 12345));

        // Test Integer
        assertEquals(123456789, schema.createNotNullConverter(new IntType()).convert(123456789));

        // Test BigInt
        assertEquals(
                123456789012345L,
                schema.createNotNullConverter(new BigIntType()).convert(123456789012345L));

        // Test Float
        assertEquals(123.45f, schema.createNotNullConverter(new FloatType()).convert(123.45f));

        // Test Double
        assertEquals(
                123456.789, schema.createNotNullConverter(new DoubleType()).convert(123456.789));

        // Test Binary
        byte[] binaryData = "testBinary".getBytes();
        assertEquals(
                binaryData, schema.createNotNullConverter(new BinaryType()).convert(binaryData));

        // Test VarBinary
        byte[] varBinaryData = "testVarBinary".getBytes();
        assertEquals(
                varBinaryData,
                schema.createNotNullConverter(new VarBinaryType(50)).convert(varBinaryData));

        // Test Char
        assertEquals("testChar", schema.createNotNullConverter(new CharType()).convert("testChar"));

        // Test Varchar
        assertEquals(
                "testVarchar",
                schema.createNotNullConverter(new VarCharType()).convert("testVarchar"));

        LocalDate testDate = LocalDate.of(2024, 1, 1);
        int daysSinceEpoch = (int) testDate.toEpochDay();
        Date expectedDate = Date.valueOf(testDate);
        assertEquals(
                expectedDate,
                schema.createNotNullConverter(new DateType()).convert(daysSinceEpoch));

        // Test TimeWithoutTimeZone
        Time time = Time.valueOf("12:34:56");
        assertEquals(
                time,
                schema.createNotNullConverter(new TimeType())
                        .convert(45296000)); // 45296000 ms equals 12:34:56

        // Test TimestampWithoutTimeZone
        TimestampData timestampData =
                TimestampData.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 12, 34, 56));
        assertEquals(
                timestampData.toTimestamp(),
                schema.createNotNullConverter(new TimestampType()).convert(timestampData));

        // Test TimestampWithTimeZone
        TimestampData timestampWithTZ =
                TimestampData.fromInstant(Instant.parse("2024-01-01T12:34:56Z"));
        assertEquals(
                "2024-01-01T12:34:56Z",
                schema.createNotNullConverter(new ZonedTimestampType()).convert(timestampWithTZ));

        // Test TimestampWithLocalTimeZone
        TimestampData timestampWithLocalTZ =
                TimestampData.fromInstant(Instant.parse("2024-01-01T12:34:56Z"));
        assertEquals(
                "2024-01-01T12:34:56Z[Etc/UTC]",
                schema.createNotNullConverter(new LocalZonedTimestampType())
                        .convert(timestampWithLocalTZ));

        // Test Decimal
        DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal("123456.789"), 9, 3);
        assertEquals(
                new BigDecimal("123456.789"),
                schema.createNotNullConverter(new DecimalType(9, 3)).convert(decimalData));

        // Test Array
        ArrayData arrayData =
                new GenericArrayData(
                        new StringData[] {
                            StringData.fromString("a"),
                            StringData.fromString("b"),
                            StringData.fromString("c")
                        });
        String expectedArrayString = "a,b,c";
        String actualArrayString =
                (String)
                        schema.createNotNullConverter(new ArrayType(new VarCharType()))
                                .convert(arrayData);
        assertEquals(expectedArrayString, actualArrayString);

        // Test Map
        Map<StringData, StringData> map = new LinkedHashMap<>();
        map.put(StringData.fromString("key1"), StringData.fromString("value1"));
        map.put(StringData.fromString("key2"), StringData.fromString("value2"));
        MapData mapData = new GenericMapData(map);
        // Assume there is a toString method that formats MapData correctly, or you can compare each
        // element
        String expectedMapString = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String actualMapString =
                (String)
                        schema.createNotNullConverter(
                                        new MapType(new VarCharType(), new VarCharType()))
                                .convert(mapData);
        assertEquals(expectedMapString, actualMapString);

        // Test Row
        RowType rowType =
                new RowType(
                        java.util.Arrays.asList(
                                new RowType.RowField("field1", new VarCharType()),
                                new RowType.RowField("field2", new IntType()),
                                new RowType.RowField("field3", new BooleanType())));
        RowData rowData = GenericRowData.of(StringData.fromString("field1"), 123, true);
        assertEquals(
                "{\"field1\":\"field1\",\"field2\":123,\"field3\":true}",
                schema.createNotNullConverter(rowType).convert(rowData));
    }

    // Create a subclass to expose the protected createNotNullConverter method
    private static class OceanBaseJsonSerializationSchemaConverterTest
            extends OceanBaseJsonSerializationSchema {

        public OceanBaseJsonSerializationSchemaConverterTest(TableInfo tableInfo) {
            super(tableInfo);
        }

        @Override
        public SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
            return super.createNotNullConverter(type);
        }
    }
}
