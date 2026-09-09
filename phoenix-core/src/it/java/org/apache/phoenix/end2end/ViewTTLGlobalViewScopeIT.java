/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scope check for how {@link org.apache.phoenix.coprocessor.CompactionScanner} applies a GLOBAL view's
 * View TTL on a multi-tenant table.
 * <p>
 * The question: can a legitimate <b>global</b> view whose {@code WHERE} pins {@code TENANT_ID = '<t>'}
 * (with a short TTL) purge that one tenant's rows? {@code getGlobalViews} loads such a view (it is a
 * valid, provenance-clean view) and its persisted ROW_KEY_MATCHER is exactly that tenant's 15-byte id
 * (confirmed by the DIAG dump below), so on its face it looks like it could.
 * <p>
 * <b>Result: it cannot, structurally.</b> On a multi-tenant, non-salted, non-index table
 * {@code CompactionScanner.getStartingPKPosition()} returns 1 and {@code getTTLExpressionForRow}
 * matches {@code GLOBAL_VIEWS} at {@code pkPositions.get(1)} — the <b>post-tenant</b> offset ("skip
 * tenant-id to search the global space", CompactionScanner.java:1285-1291,1353-1356). A global view's
 * matcher valued at the tenant id is therefore compared against the row bytes <em>after</em> the
 * tenant id and can never match; only the {@code TENANT_VIEWS} path (offset 0) is applied at the
 * tenant prefix. So a global-view TTL cannot single out one tenant's rows; it applies to the global /
 * all-tenant key space, i.e. a global view's TTL has no cross-tenant row-targeting reach. (A stalled
 * compaction is inherently table-wide, but that is availability, separate from row targeting.)
 * <p>
 * The test is a differential holding the setup constant and varying only what the global view pins:
 * <ul>
 * <li><b>tenant-id pin</b> ({@code WHERE TENANT_ID='<t>'}) — matcher = the tenant id, compared at the
 * post-tenant offset, so it never matches and that tenant's rows survive;</li>
 * <li><b>post-tenant-key pin</b> ({@code WHERE KP='AAA'}, the shared key the rows carry) — matcher =
 * the post-tenant key bytes, compared at the post-tenant offset, so it DOES purge. This is the
 * positive control: it proves the harness deletes and that global-view View TTL applies to the global
 * / all-tenant key space (not to a tenant prefix), which is precisely why the tenant-id pin is
 * powerless to single out a tenant.</li>
 * </ul>
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLGlobalViewScopeIT extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLGlobalViewScopeIT.class);

  private static final int TTL_SECS = 10;
  private static final int NUM_ROWS = 5;
  /** Shared post-tenant leading PK value the target's rows carry (KP column). */
  private static final String KEY_PREFIX = "AAA";

  private ManualEnvironmentEdge injectEdge;
  private int tenantSeq = 0;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    // View TTL + compaction on, no max lookback so expired rows are physically removed on major
    // compaction.
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.PHOENIX_COMPACTION_ENABLED, String.valueOf(true));
    props.put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
    props.put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(false));
    props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(0));
    props.put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(20));
    setUpTestDriver(new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS, props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  @After
  public synchronized void afterTest() {
    EnvironmentEdgeManager.reset();
  }

  /**
   * A legitimate global view pinning a tenant's {@code TENANT_ID} with a short TTL must NOT purge that
   * tenant's rows (global-view matchers are applied at the post-tenant offset), while the same global
   * view pinning the shared post-tenant key (KP) DOES purge. The differential proves the scoping is
   * real and not a dead harness.
   */
  @Test
  public void testGlobalViewTenantIdPinCannotTargetTenantRows() throws Exception {
    // Run both arms before asserting so the DIAG catalog dump is captured for each regardless of
    // outcome (records the actual persisted ROW_KEY_MATCHER that underpins the ruling).
    int tenantIdPinSurvivors = runGlobalViewScenario(true);
    int keyPinSurvivors = runGlobalViewScenario(false);
    LOGGER.info("DIAG RESULT tenantIdPinSurvivors={} keyPinSurvivors={}", tenantIdPinSurvivors,
      keyPinSurvivors);

    assertEquals("A global view pinning a tenant's TENANT_ID + short TTL must NOT "
      + "purge that tenant's rows: on a multi-tenant table global-view matchers are compared at the "
      + "post-tenant offset (getStartingPKPosition=1), so a tenant-id-valued matcher never matches",
      NUM_ROWS, tenantIdPinSurvivors);

    assertEquals("Positive control: the same global view pinning the shared post-tenant key (KP) "
      + "DOES purge — its matcher is compared at the post-tenant offset and matches. This proves the "
      + "harness deletes and that global-view TTL targets the global/all-tenant key space, not a "
      + "tenant prefix (which is why the tenant-id pin above cannot single out a tenant)", 0,
      keyPinSurvivors);
  }

  /**
   * Creates a multi-tenant base table, writes {@link #NUM_ROWS} rows under a target tenant through an
   * ordinary no-TTL tenant view (rows keyed {@code [targetTenant][KP='AAA'][ID]}), then creates a
   * legitimate GLOBAL view with a short TTL whose {@code WHERE} pins either the target's
   * {@code TENANT_ID} ({@code pinTenantId=true}) or the shared post-tenant key {@code KP}
   * ({@code pinTenantId=false}). Advances the clock past the TTL, major-compacts the physical table,
   * and returns the surviving raw HBase row count.
   *
   * @param pinTenantId whether the global view pins {@code TENANT_ID} (vs the shared post-tenant KP)
   * @return count of surviving rows in the physical table after compaction
   */
  private int runGlobalViewScenario(boolean pinTenantId) throws Exception {
    // Start from a real (non-frozen) clock so the global view's ALTER SET TTL persists at wall-clock
    // time (avoids the frozen-clock artifact where a TTL silently fails to persist).
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());

    String targetTenant = nextTenantId();
    String baseTable = generateUniqueName();
    String targetView = "VV" + generateUniqueName();
    String globalView = "GV" + generateUniqueName();

    try (Connection global = DriverManager.getConnection(getUrl());
      Statement stmt = global.createStatement()) {
      stmt.execute(createTableDdl(baseTable));
    }

    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();

    // Target rows under the target tenant, via a plain no-TTL tenant view. Physical keys are
    // [targetTenant(15)][KP='AAA'(3)][ID(5)] -> absent a matching TTL they survive compaction.
    try (Connection tenant = DriverManager.getConnection(tenantUrl(targetTenant))) {
      try (Statement stmt = tenant.createStatement()) {
        stmt.execute("CREATE VIEW " + targetView + " AS SELECT * FROM " + baseTable + " WHERE KP = '"
          + KEY_PREFIX + "'");
      }
      try (Statement stmt = tenant.createStatement()) {
        for (int i = 1; i <= NUM_ROWS; i++) {
          stmt.execute(
            String.format("UPSERT INTO %s (ID, NUM) VALUES ('R%04d', %d)", targetView, i, i));
        }
      }
      tenant.commit();
    }

    // A legitimate GLOBAL view with a short TTL. tenant-id pin -> matcher = the tenant id (matched at
    // the post-tenant offset -> never matches); post-tenant KP pin -> matcher = the KP bytes (matched
    // at the post-tenant offset -> matches -> purges).
    String pinExpr =
      pinTenantId ? ("TENANT_ID = '" + targetTenant + "'") : ("KP = '" + KEY_PREFIX + "'");
    try (Connection global = DriverManager.getConnection(getUrl());
      Statement stmt = global.createStatement()) {
      stmt.execute(
        "CREATE VIEW " + globalView + " AS SELECT * FROM " + baseTable + " WHERE " + pinExpr);
      stmt.execute("ALTER VIEW " + globalView + " SET TTL='" + TTL_SECS + "'");
    }

    // DIAGNOSTIC + ASSERTION: capture how the global view is actually stored (TTL + ROW_KEY_MATCHER)
    // and assert it persisted as the ruling assumes, so the survive/purge outcome is tied to a
    // verified live matcher rather than a silently-dead TTL.
    dumpAndAssertCatalogState(baseTable, globalView, pinTenantId ? targetTenant : KEY_PREFIX);

    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (TTL_SECS * 1000L);
    majorCompactBaseTable(baseTable, scnTimestamp);

    int surviving = rawRowCount(baseTable, earliestTimestamp);
    LOGGER.info("Global-view scenario pinTenantId={} targetTenant={} "
      + "-> surviving rows={}", pinTenantId, targetTenant, surviving);
    return surviving;
  }

  /**
   * Logs AND asserts how the global view is stored in SYSTEM.CATALOG: its header row
   * (TABLE_TYPE='v') carrying TTL + ROW_KEY_MATCHER, and the physical-link rows
   * {@code getGlobalViews} scans (LINK_TYPE=2, COLUMN_FAMILY=base table). Read on a global
   * (non-tenant) connection, mirroring the server path.
   * <p>
   * The assertions turn this from a bare diagnostic into a load-bearing check: the containment ruling
   * only holds if the global view's TTL actually persisted (a silently-dead TTL would make the
   * tenant-id-pin arm "survive" for the wrong reason — a false-green) and if its ROW_KEY_MATCHER is
   * exactly the pinned value's bytes (the matcher the offset analysis reasons about). Both are
   * asserted here so the survive/purge outcome is anchored to verified catalog state.
   */
  private void dumpAndAssertCatalogState(String baseTable, String globalView, String pinnedValue)
    throws SQLException {
    try (Connection global = DriverManager.getConnection(getUrl());
      Statement stmt = global.createStatement()) {
      LOGGER.info("DIAG pinnedValue='{}' (bytes={})", pinnedValue,
        Bytes.toStringBinary(Bytes.toBytes(pinnedValue)));
      boolean sawHeaderRow = false;
      String headerTtl = null;
      byte[] headerMatcher = null;
      try (ResultSet rs = stmt.executeQuery("SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, "
        + "TTL, ROW_KEY_MATCHER FROM SYSTEM.CATALOG WHERE TABLE_NAME = '" + globalView + "'")) {
        while (rs.next()) {
          byte[] m = rs.getBytes("ROW_KEY_MATCHER");
          String type = rs.getString("TABLE_TYPE");
          String ttl = rs.getString("TTL");
          LOGGER.info("DIAG catalog row: tid={} schem={} name={} type={} ttl={} matcherLen={} "
            + "matcher={}", rs.getString("TENANT_ID"), rs.getString("TABLE_SCHEM"),
            rs.getString("TABLE_NAME"), type, ttl, m == null ? -1 : m.length,
            m == null ? "null" : Bytes.toStringBinary(m));
          // The view's header row (TABLE_TYPE='v') is where the persisted TTL + ROW_KEY_MATCHER live.
          if ("v".equals(type)) {
            sawHeaderRow = true;
            headerTtl = ttl;
            headerMatcher = m;
          }
        }
      }
      try (ResultSet rs = stmt.executeQuery("SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, LINK_TYPE, "
        + "COLUMN_FAMILY, TABLE_TYPE FROM SYSTEM.CATALOG WHERE COLUMN_FAMILY = '" + baseTable
        + "' AND LINK_TYPE = 2")) {
        while (rs.next()) {
          LOGGER.info("DIAG physical-link row: tid={} schem={} name={} linkType={} colFam={} "
            + "type={}", rs.getString("TENANT_ID"), rs.getString("TABLE_SCHEM"),
            rs.getString("TABLE_NAME"), rs.getString("LINK_TYPE"), rs.getString("COLUMN_FAMILY"),
            rs.getString("TABLE_TYPE"));
        }
      }

      // Assert the harness is live, not silently dead. The ruling rests on two concrete catalog facts:
      // (1) the global view's TTL persisted at exactly TTL_SECS (a null/other value would mean the
      // ALTER SET TTL silently failed under a frozen clock -> the tenant-id arm would "survive" for
      // the wrong reason); (2) its ROW_KEY_MATCHER equals the pinned value's bytes (only because the
      // tenant-id-pin matcher IS the 15-byte tenant id, compared at the post-tenant offset, is it
      // structurally unable to match a tenant's rows).
      assertTrue("expected a header row (TABLE_TYPE='v') for global view " + globalView
        + " carrying its persisted TTL + ROW_KEY_MATCHER", sawHeaderRow);
      assertEquals("global view " + globalView + " must persist TTL=" + TTL_SECS
        + " (a null/other value would mean the ALTER SET TTL silently failed -> false-green)",
        String.valueOf(TTL_SECS), headerTtl);
      assertNotNull("global view " + globalView + " must persist a non-null ROW_KEY_MATCHER",
        headerMatcher);
      assertTrue("global view " + globalView + " ROW_KEY_MATCHER must be non-empty",
        headerMatcher.length > 0);
      assertArrayEquals("global view " + globalView + " ROW_KEY_MATCHER must equal the pinned "
        + "value's bytes ('" + pinnedValue + "') — the matcher the containment analysis reasons about",
        Bytes.toBytes(pinnedValue), headerMatcher);
    }
  }

  private static String createTableDdl(String tableName) {
    return "CREATE TABLE " + tableName + " (TENANT_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, "
      + "ID CHAR(5) NOT NULL, NUM INTEGER "
      + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, KP, ID)) "
      + "MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'";
  }

  private String nextTenantId() {
    return String.format("T%014d", ++tenantSeq);
  }

  private String tenantUrl(String tenantId) {
    return getUrl() + ';' + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
  }

  /** Flush, freeze the clock at {@code scnTimestamp}, then major-compact and wait for completion. */
  private void majorCompactBaseTable(String tableName, long scnTimestamp) throws Exception {
    TableName table = TableName.valueOf(tableName);
    try (org.apache.hadoop.hbase.client.Connection connection =
      ConnectionFactory.createConnection(getUtility().getConfiguration())) {
      Admin admin = connection.getAdmin();
      if (!admin.tableExists(table)) {
        return;
      }
      admin.flush(table);
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(scnTimestamp);
      TestUtil.majorCompact(getUtility(), table);
    }
  }

  /** Counts rows in the physical HBase table via a raw scan (mirrors BaseViewTTLIT). */
  private int rawRowCount(String tableName, long minTimestamp) throws IOException, SQLException {
    byte[] hbaseTableName = Bytes.toBytes(tableName);
    try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
      .getTable(hbaseTableName)) {
      Scan scan = new Scan();
      scan.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
      try (ResultScanner scanner = tbl.getScanner(scan)) {
        int numRows = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          numRows++;
        }
        return numRows;
      }
    }
  }
}
