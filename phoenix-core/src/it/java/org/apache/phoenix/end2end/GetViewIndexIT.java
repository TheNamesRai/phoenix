package org.apache.phoenix.end2end;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
public class GetViewIndexIT extends ParallelStatsDisabledIT {
    @Test
    public void test() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("create table T "+
                    " (id INTEGER not null primary key, v1 varchar, v2 varchar, v3 varchar)");
            conn.createStatement().execute("CREATE VIEW V AS SELECT * FROM T WHERE v1 = 'a'");
            conn.createStatement().execute("CREATE INDEX I ON V (v2) INCLUDE (v3)");
            conn.createStatement().execute("upsert into V values (1, 'a', 'ab', 'abc')");
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT v1, v3 from V WHERE v2  = 'ab'");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("a", rs.getString("v1"));
        }
    }
}
