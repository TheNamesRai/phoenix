package org.apache.phoenix.end2end;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;
import static org.junit.Assert.*;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class DeleteColumnUncoveredIndex extends BaseTest {

    private final boolean uncovered;
    private final boolean salted;
    public DeleteColumnUncoveredIndex (boolean uncovered, boolean salted) {
        this.uncovered = uncovered;
        this.salted = salted;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void unsetFailForTesting() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        assertFalse("refCount leaked", refCountLeaked);
    }
    @Parameterized.Parameters(
            name = "uncovered={0},salted={1}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false }, { false, true }, { true, false }, { true, true }
        });
    }
    @Test
    public void testDDL() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), " +
                    " val3 varchar(10))");
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
            Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
            conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexTableName
                    + " on " + dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();


            // Write a query to get the val2 = 'bc' with a time range query
            String query = "SELECT "
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName
                    + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('"
                    + before.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '"
                    + timeZoneID + "') AND " + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
        }

    }
}
