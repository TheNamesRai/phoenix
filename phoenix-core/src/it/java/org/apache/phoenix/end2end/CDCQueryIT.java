/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import com.google.gson.Gson;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.end2end.index.SingleCellIndexIT.dumpTable;
import static org.apache.phoenix.query.QueryConstants.CDC_CHANGE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCQueryIT extends CDCBaseIT {
    private final boolean forView;
    private final boolean dataFirst;
    private final PTable.QualifierEncodingScheme encodingScheme;
    private ManualEnvironmentEdge injectEdge;

    public CDCQueryIT(Boolean forView, Boolean dataFirst,
                      PTable.QualifierEncodingScheme encodingScheme) {
        this.forView = forView;
        this.dataFirst = dataFirst;
        this.encodingScheme = encodingScheme;
    }

    @Parameterized.Parameters(name = "forView={0} dataFirst={1}, encodingScheme={2}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.valueOf(false), Boolean.valueOf(false), TWO_BYTE_QUALIFIERS },
                // testSelect() is getting stuck.
                //{ Boolean.valueOf(false), Boolean.valueOf(true), TWO_BYTE_QUALIFIERS },
                { Boolean.valueOf(false), Boolean.valueOf(false), NON_ENCODED_QUALIFIERS },
                // Getting: TableNotFoundException: ERROR 1012 (42M03): Table undefined.
                { Boolean.valueOf(true), Boolean.valueOf(false), TWO_BYTE_QUALIFIERS },
        });
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    private void assertResultSet(ResultSet rs, Set<PTable.CDCChangeScope> cdcChangeScopeSet) throws Exception{
        Gson gson = new Gson();
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 100d);
            postImage.put("V2", 1000d);
            row1.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 100d);
            changeImage.put("V2", 1000d);
            row1.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row1.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        Map<String, Object> row2 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 200d);
            postImage.put("V2", 2000d);
            row2.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 200d);
            changeImage.put("V2", 2000d);
            row2.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row2.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row2, gson.fromJson(rs.getString(3),
                HashMap.class));
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row3 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 101d);
            postImage.put("V2", 1000d);
            row3.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 101d);
            row3.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 100d);
            preImage.put("V2", 1000d);
            row3.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row3, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row4 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row4.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row4.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 101d);
            preImage.put("V2", 1000d);
            row4.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row4, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row5 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 102d);
            postImage.put("V2", 1002d);
            row5.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 102d);
            changeImage.put("V2", 1002d);
            row5.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            row5.put(CDC_PRE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        assertEquals(row5, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row6 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row6.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row6.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 102d);
            preImage.put("V2", 1002d);
            row6.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row6, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        Map<String, Object> row7 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 201d);
            postImage.put("V2", null);
            row7.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 201d);
            changeImage.put("V2", null);
            row7.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 200d);
            preImage.put("V2", 2000d);
            row7.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row7, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row8 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 103d);
            postImage.put("V2", 1003d);
            row8.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 103d);
            changeImage.put("V2", 1003d);
            row8.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            row8.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row8, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row9 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row9.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row9.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 103d);
            preImage.put("V2", 1003d);
            row9.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row9, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row10 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            Map<String, Double> postImage = new HashMap<>();
            postImage.put("V1", 104d);
            postImage.put("V2", 1004d);
            row10.put(CDC_POST_IMAGE, postImage);
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            Map<String, Double> changeImage = new HashMap<>();
            changeImage.put("V1", 104d);
            changeImage.put("V2", 1004d);
            row10.put(CDC_CHANGE_IMAGE, changeImage);
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            row10.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row10, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));
        Map<String, Object> row11 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_DELETE_EVENT_TYPE);
        }};
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.POST)) {
            row11.put(CDC_POST_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet == null || cdcChangeScopeSet.contains(PTable.CDCChangeScope.CHANGE)) {
            row11.put(CDC_CHANGE_IMAGE, new HashMap<String, Double>() {{
            }});
        }
        if (cdcChangeScopeSet != null && cdcChangeScopeSet.contains(PTable.CDCChangeScope.PRE)) {
            Map<String, Double> preImage = new HashMap<>();
            preImage.put("V1", 104d);
            preImage.put("V2", 1004d);
            row11.put(CDC_PRE_IMAGE, preImage);
        }
        assertEquals(row11, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(false, rs.next());
        rs.close();
    }

    @Test
    public void testSelectCDC() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTable(conn, "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 " +
                "INTEGER, v2 INTEGER)", encodingScheme);
        if (forView) {
            String viewName = generateUniqueName();
            createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                    encodingScheme);
            tableName = viewName;
        }
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        if (! dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        // NOTE: To debug the query execution, add the below condition where you need a breakpoint.
        //      if (<table>.getTableName().getString().equals("N000002") ||
        //                 <table>.getTableName().getString().equals("__CDC__N000002")) {
        //          "".isEmpty();
        //      }
        if (dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }

        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName +
                " WHERE \"PHOENIX_ROW_TIMESTAMP()\" < NOW()"), null);
        assertResultSet(conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(PRE, POST) */ * " +
                "FROM " + cdcName), new HashSet<PTable.CDCChangeScope>(
                        Arrays.asList(PTable.CDCChangeScope.PRE, PTable.CDCChangeScope.POST)));
        assertResultSet(conn.createStatement().executeQuery("SELECT " +
                "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcName), null);

        HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
            put("SELECT 'dummy', k FROM " + cdcName, new int [] {1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k ASC", new int [] {1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY k DESC", new int [] {2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1});
            put("SELECT * FROM " + cdcName +
                    " ORDER BY PHOENIX_ROW_TIMESTAMP() ASC", new int [] {1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1});
        }};
        for (Map.Entry<String, int[]> testQuery: testQueries.entrySet()) {
            try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
                for (int k:  testQuery.getValue()) {
                    assertEquals(true, rs.next());
                    assertEquals(k, rs.getInt(2));
                }
                assertEquals(false, rs.next());
            }
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() > NOW()")) {
            assertEquals(false, rs.next());
        }
    }

    @Test
    public void testSelectTimeRangeQueries() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTable(conn, "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 " +
                "INTEGER)", encodingScheme);
        if (forView) {
            String viewName = generateUniqueName();
            createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                    encodingScheme);
            tableName = viewName;
        }
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        createCDCAndWait(conn, tableName, cdcName, cdc_sql, null);
        if (! dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }
        Timestamp ts1 = new Timestamp(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(ts1.getTime());
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(System.currentTimeMillis());
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
        conn.commit();
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
        conn.commit();
        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 300);
        Timestamp ts2 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (3, 300)");
        conn.commit();
        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200);
        Timestamp ts3 = new Timestamp(cal.getTime().getTime());
        dumpTable(CDCUtil.getCDCIndexName(cdcName));
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k = 2");
        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 100);
        Timestamp ts4 = new Timestamp(cal.getTime().getTime());
        conn.commit();
        if (dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }

//        dumpTable(CDCUtil.getCDCIndexName(cdcName));
//        String sql = "SELECT * FROM " + cdcName + " WHERE \"PHOENIX_ROW_TIMESTAMP()\" <= ?";
//        PreparedStatement stmt2 = conn.prepareStatement(sql);
//        //assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() <= ?"), null);
//
//        stmt2.setTimestamp(1, ts2);
//        ResultSet rs2 = stmt2.executeQuery();
//        assertEquals(true, rs2.next());
        String sel_sql = "SELECT * FROM " + cdcName + " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND " +
                "PHOENIX_ROW_TIMESTAMP() <= ?";

        Object[] testDataSets = new Object[] {
                new Object[] {ts1, ts2, new int[] {1, 2}}/*,
                new Object[] {ts2, ts3, new int[] {1, 3}},
                new Object[] {ts3, ts4, new int[] {1}}*/
        };
        PreparedStatement stmt = conn.prepareStatement(sel_sql);
        for (int i = 0; i < testDataSets.length; ++i) {
            Object[] testData = (Object[]) testDataSets[i];
            stmt.setTimestamp(1, (Timestamp) testData[0]);
            stmt.setTimestamp(2, (Timestamp) testData[1]);
            try (ResultSet rs = stmt.executeQuery()) {
                for (int k:  (int[]) testData[2]) {
                    assertEquals(true, rs.next());
                    assertEquals(k, rs.getInt(2));
                }
                assertEquals(false, rs.next());
            }
        }
    }

    @Test
    public void testSelectCDCWithDDL() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTable(conn, "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v0 " +
                "INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, v3 INTEGER)", encodingScheme);
        if (forView) {
            String viewName = generateUniqueName();
            createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                    encodingScheme);
            tableName = viewName;
        }

        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        if (! dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }

        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v0");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v3");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        conn.commit();
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        conn.commit();
        Thread.sleep(10);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN v1v2");
        conn.commit();
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD v4 INTEGER");
        conn.commit();
        if (dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }

        assertResultSet(conn.createStatement().executeQuery("SELECT * FROM " + cdcName), null);
    }

    private void assertCDCBinaryAndDateColumn(ResultSet rs,
                                              List<byte []> byteColumnValues,
                                              List<Date> dateColumnValues,
                                              Timestamp timestamp) throws Exception {
        assertEquals(true, rs.next());
        assertEquals(1, rs.getInt(2));

        Gson gson = new Gson();
        Map<String, Object> row1 = new HashMap<String, Object>(){{
            put(CDC_EVENT_TYPE, CDC_UPSERT_EVENT_TYPE);
        }};
        Map<String, Object> postImage = new HashMap<>();
        postImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        postImage.put("D", dateColumnValues.get(0).toString());
        postImage.put("T", timestamp.toString());
        row1.put(CDC_POST_IMAGE, postImage);
        Map<String, Object> changeImage = new HashMap<>();
        changeImage.put("A_BINARY",
                Base64.getEncoder().encodeToString(byteColumnValues.get(0)));
        changeImage.put("D", dateColumnValues.get(0).toString());
        changeImage.put("T", timestamp.toString());
        row1.put(CDC_CHANGE_IMAGE, changeImage);
        row1.put(CDC_PRE_IMAGE, new HashMap<String, String>() {{
        }});
        assertEquals(row1, gson.fromJson(rs.getString(3),
                HashMap.class));

        assertEquals(true, rs.next());
        assertEquals(2, rs.getInt(2));
        HashMap<String, Object> row2Json = gson.fromJson(rs.getString(3), HashMap.class);
        String row2BinaryColStr = (String) ((Map)((Map)row2Json.get(CDC_CHANGE_IMAGE))).get("A_BINARY");
        byte[] row2BinaryCol = Base64.getDecoder().decode(row2BinaryColStr);

        assertEquals(0, DescVarLengthFastByteComparisons.compareTo(byteColumnValues.get(1),
                0, byteColumnValues.get(1).length, row2BinaryCol, 0, row2BinaryCol.length));
    }

    @Test
    public void testCDCBinaryAndDateColumn() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        List<byte []> byteColumnValues = new ArrayList<>();
        byteColumnValues.add( new byte[] {0,0,0,0,0,0,0,0,0,1});
        byteColumnValues.add(new byte[] {0,0,0,0,0,0,0,0,0,2});
        List<Date> dateColumnValues = new ArrayList<>();
        dateColumnValues.add(Date.valueOf("2024-02-01"));
        dateColumnValues.add(Date.valueOf("2024-01-31"));
        Timestamp timestampColumnValue = Timestamp.valueOf("2024-01-31 12:12:14");
        try {
            createTable(conn, "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " " +
                    "a_binary binary(10), d Date, t TIMESTAMP)", encodingScheme);
            if (forView) {
                String viewName = generateUniqueName();
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            String cdcName = generateUniqueName();
            String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (! dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
            }

            String upsertQuery = "UPSERT INTO " + tableName + " (k, a_binary, d, t) VALUES (?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsertQuery);
            stmt.setInt(1, 1);
            stmt.setBytes(2, byteColumnValues.get(0));
            stmt.setDate(3, dateColumnValues.get(0));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setBytes(2, byteColumnValues.get(1));
            stmt.setDate(3, dateColumnValues.get(1));
            stmt.setTimestamp(4, timestampColumnValue);
            stmt.execute();
            conn.commit();
            if (dataFirst) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
            }

            assertCDCBinaryAndDateColumn(conn.createStatement().executeQuery
                    ("SELECT /*+ CDC_INCLUDE(PRE, POST, CHANGE) */ * " + "FROM " + cdcName),
                    byteColumnValues, dateColumnValues, timestampColumnValue);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectCDCFailDataTableUpdate() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTable(conn, "CREATE TABLE  " + tableName +
                " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 INTEGER)", encodingScheme);
        if (forView) {
            String viewName = generateUniqueName();
            createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                    encodingScheme);
            tableName = viewName;
        }
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName
                + " ON " + tableName;
        if (! dataFirst) {
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }
        IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
        assertCDCState(conn, cdcName, null, 3);
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(System.currentTimeMillis());
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 100, 1000)");
        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 200, 2000)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 102, 1002)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (2, 201, NULL)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 103, 1003)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1, v2) VALUES (1, 104, 1004)");
        commitWithException(conn);

        injectEdge.incrementValue(100);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k=1");
        commitWithException(conn);
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);

        if (dataFirst) {
            EnvironmentEdgeManager.reset();
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme);
        }

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + cdcName);
        assertEquals(false, rs.next());
    }

    static private void commitWithException(Connection conn) {
        try {
            conn.commit();
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            fail();
        } catch (Exception e) {
            // this is expected
        }
    }

}
