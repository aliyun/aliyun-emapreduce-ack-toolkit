package com.aliyun.emr.ack.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;

public class SqlTest {

    @Test
    public void parseStatements_splitsMultipleStatementsAndDropsEmptyOnes() {
        List<String> statements = Sql.parseStatements("SELECT 1;;\nSELECT 2;  ; SELECT 3");

        assertEquals(3, statements.size());
        assertEquals("SELECT 1", statements.get(0));
        assertEquals("SELECT 2", statements.get(1));
        assertEquals("SELECT 3", statements.get(2));
    }

    @Test
    public void parseStatements_ignoresSemicolonsInsideSingleAndDoubleQuotes() {
        List<String> statements =
                Sql.parseStatements("SELECT ';' AS semi, \"a;b\" AS quoted; SELECT 'done'");

        assertEquals(2, statements.size());
        assertEquals("SELECT ';' AS semi, \"a;b\" AS quoted", statements.get(0));
        assertEquals("SELECT 'done'", statements.get(1));
    }

    @Test
    public void parseStatements_ignoresSemicolonsInsideComments() {
        List<String> statements =
                Sql.parseStatements(
                        "-- ignored ;\n" + "SELECT 1; /* ignored ; still ignored */ SELECT 2;");

        assertEquals(2, statements.size());
        assertEquals("SELECT 1", statements.get(0));
        assertEquals("SELECT 2", statements.get(1));
    }

    @Test
    public void parseStatements_preservesEscapedQuotesInsideStrings() {
        List<String> statements =
                Sql.parseStatements(
                        "SELECT 'it\\'s; still one string' AS value; SELECT \"a\\\";b\" AS value");

        assertEquals(2, statements.size());
        assertEquals("SELECT 'it\\'s; still one string' AS value", statements.get(0));
        assertEquals("SELECT \"a\\\";b\" AS value", statements.get(1));
    }

    @Test
    public void parseStatements_commentOnlyInputProducesNoStatements() {
        List<String> statements = Sql.parseStatements("-- only a comment;\n/* and another ; */");

        assertTrue(statements.isEmpty());
    }
}
