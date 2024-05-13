package com.oceanbase.connector.flink.dialect;

import org.junit.Assert;
import org.junit.Test;

public class MySQLDialectUnitTest {

    @Test
    public void testQuoteIdentifier() {
        String identifier = "name";
        String quoteStr = "`" + identifier.replaceAll("`", "``") + "`";
        Assert.assertEquals(quoteStr, String.format("`%s`", identifier));
    }
}
