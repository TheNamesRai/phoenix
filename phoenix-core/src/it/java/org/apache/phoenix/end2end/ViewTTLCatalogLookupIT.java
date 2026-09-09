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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
 * Regression suite for the View TTL catalog lookup in
 * {@link org.apache.phoenix.coprocessor.CompactionScanner}. Every test asserts the behavior the fixed
 * {@code getTTLInfo} must exhibit when it looks up each view's {@code TTL} / {@code ROW_KEY_MATCHER}
 * during major compaction, and is expected to pass only against a {@code phoenix-core-server} built
 * with the fix applied.
 * <p>
 * The fix has three parts, each guarded by one test below:
 * <ol>
 * <li><b>Parameterized IN-list</b> — {@code getTTLInfo} binds each view's
 * {@code (TENANT_ID, TABLE_SCHEM, TABLE_NAME)} as data instead of concatenating the raw identifiers,
 * so a view whose name contains a single quote can neither abort the query with a syntax error
 * ({@link #testUnusualViewNameHandledDuringCompaction}) nor add an extra row-value tuple to the list
 * ({@link #testUnrelatedViewMetadataNotAppliedDuringCompaction}).</li>
 * <li><b>Provenance check</b> — a returned {@code (tenant, schema, name)} triple is honored only if it
 * is one of the views actually linked to the physical table being compacted, so no unrelated view's
 * TTL / ROW_KEY_MATCHER can be applied ({@link #testUnrelatedViewMetadataNotAppliedDuringCompaction}).</li>
 * <li><b>Null-matcher guard</b> — a view carrying a TTL but a null/empty ROW_KEY_MATCHER is skipped
 * rather than dereferenced, so it can no longer NPE and abort the entire major compaction
 * ({@link #testViewWithNullMatcherHandledDuringCompaction}).</li>
 * </ol>
 * Two positive controls confirm the fix does not regress legitimate View TTL enforcement:
 * {@link #testBaselineViewTtlStillPurges} (a tenant view) and
 * {@link #testGlobalViewNullTenantTtlStillPurges} (a GLOBAL / null-tenant view). The latter guards the
 * part of the parameterization most at risk of a matching regression: for a global view the old code
 * inlined the bare {@code NULL} keyword for {@code TENANT_ID}/{@code TABLE_SCHEM}, whereas the fix binds
 * them via {@code setNull} — if bound NULLs did not match the null-tenant catalog row the way the
 * inlined keyword did, global-view TTL would silently stop purging.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLCatalogLookupIT extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLCatalogLookupIT.class);

  /** View TTL in seconds; the compaction clock is advanced by this amount to force expiry. */
  private static final int TTL_SECS = 10;
  private static final int NUM_ROWS = 5;
  /** Value the legit view pins its leading (post-tenant) PK column to, giving a non-null matcher. */
  private static final String KEY_PREFIX = "AAA";
  /** A second key prefix carried by rows NOT covered by any view/TTL — must always survive. */
  private static final String NON_MATCHING_KEY_PREFIX = "ZZZ";

  private ManualEnvironmentEdge injectEdge;
  private int tenantSeq = 0;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    // View TTL + compaction on, no max lookback so expired rows are physically removed, and a raised
    // per-scan limit so co-located views share one IN(...) list (co-location is what makes a view's
    // identifiers share the TTL lookup with its siblings).
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

  // ---------------------------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------------------------

  /**
   * Positive control (no regression): a legitimate short-TTL view still purges its rows on major
   * compaction after the fix. This is also the suite's aliveness anchor — it proves the harness can
   * delete when a legitimate TTL applies, so the "rows survive" results below are meaningful.
   */
  @Test
  public void testBaselineViewTtlStillPurges() throws Exception {
    int surviving = runCoLocatedViewScenario(SecondView.NONE);
    assertEquals("Fix must not regress legitimate View TTL: the legit view should still purge all "
      + "rows on compaction", 0, surviving);
  }

  /**
   * Positive control #2 — no regression on the GLOBAL (null-tenant) view path. The fix binds each
   * view's {@code (TENANT_ID, TABLE_SCHEM, TABLE_NAME)} through a parameterized {@code (?, ?, ?)}
   * IN-list. For a global view the earlier code inlined the bare {@code NULL} keyword for
   * {@code TENANT_ID}/{@code TABLE_SCHEM} (which empirically matched the null-tenant catalog row),
   * whereas the fix binds those two positions via {@code setNull(Types.VARCHAR)}. If a bound NULL did
   * not match that row the way the inlined keyword did, {@code getTTLInfo} would stop returning global
   * views and their TTL would silently no longer purge — a fail-safe but real functional regression,
   * and the one untested by the tenant-view tests above (which only ever null-bind the middle schema
   * column, never the leading tenant column). This test proves global-view TTL still purges post-fix.
   * <p>
   * A non-multi-tenant base table makes the view unambiguously global (null {@code TENANT_ID}) with its
   * matcher applied at row-key offset 0. Two row sets are written: {@code KP='AAA'} (covered by the
   * global view's {@code WHERE KP='AAA'} + TTL) and {@code KP='ZZZ'} (no view / no TTL). Post-compaction
   * the AAA rows must be purged (global-view TTL still applied) while the ZZZ rows must survive — the
   * survivors double as the aliveness+discrimination anchor, proving the purge is a surgical matcher
   * hit rather than a blanket delete or an empty-matcher wildcard.
   */
  @Test
  public void testGlobalViewNullTenantTtlStillPurges() throws Exception {
    int[] survivors = runGlobalViewPurgeScenario();
    int coveredSurvivors = survivors[0];   // KP='AAA' rows, prefixed by the global view's matcher
    int uncoveredSurvivors = survivors[1]; // KP='ZZZ' rows, not covered by any TTL
    assertEquals("Fix must not regress GLOBAL (null-tenant) View TTL: rows covered by the global "
      + "view's WHERE KP='" + KEY_PREFIX + "' + TTL must still be purged. A non-zero count means the "
      + "setNull-bound NULL tenant/schema no longer match the null-tenant catalog row that the old "
      + "inlined NULL keyword matched — a regression in the parameterized IN-list.", 0,
      coveredSurvivors);
    assertEquals("Uncovered rows (KP='" + NON_MATCHING_KEY_PREFIX + "', no view/TTL) must survive: "
      + "proves the purge above is a surgical matcher hit, not a blanket delete or empty-matcher "
      + "wildcard, and that the compaction harness is alive.", NUM_ROWS, uncoveredSurvivors);
  }

  /**
   * Provenance: a second view whose balanced name would append an unrelated view's tuple to the lookup
   * can no longer make compaction apply that view's TTL + ROW_KEY_MATCHER. The parameterized IN-list
   * binds the name as data (so the extra tuple is never appended) and the provenance check would drop
   * it regardless, so the target's rows — which have no TTL of their own — survive.
   */
  @Test
  public void testUnrelatedViewMetadataNotAppliedDuringCompaction() throws Exception {
    // Control: no second view -> the target's own view has no TTL, so its rows survive.
    int controlSurvivors = runProvenanceScenario(false);
    assertEquals("With no second view, the target's rows (no TTL of their own) must survive", NUM_ROWS,
      controlSurvivors);

    // With the second view present, the unrelated view's policy is NOT applied, so the target's rows
    // still survive.
    int otherViewSurvivors = runProvenanceScenario(true);
    assertEquals("An unrelated view's tuple must not delete the target's rows; the parameterized query "
      + "+ provenance check prevent an unrelated view's TTL/matcher from being applied", NUM_ROWS,
      otherViewSurvivors);
  }

  /**
   * Syntax-abort avoided (name vector): a co-located view whose name contains a single quote no longer
   * corrupts the compaction-time TTL query. Because the name is now bound as a parameter, there is no
   * {@code ERROR 604} syntax error to abort the compaction, so the legitimate co-located view's TTL is
   * applied and its rows are purged as normal.
   */
  @Test
  public void testUnusualViewNameHandledDuringCompaction() throws Exception {
    int surviving = runCoLocatedViewScenario(SecondView.QUOTED_NAME);
    assertEquals("A single-quote view name must no longer abort compaction; the legit view's TTL "
      + "should still purge all rows", 0, surviving);
  }

  /**
   * Null-matcher guard (defense in depth): a co-located view that carries a TTL but a null/empty
   * ROW_KEY_MATCHER (a bare {@code SELECT *} view that never pinned a leading PK column) is now skipped
   * instead of dereferenced. Pre-fix it NPEs in {@code RowKeyMatcher.put} and aborts the whole major
   * compaction, so a co-located legit view's rows survive; post-fix compaction proceeds and the legit
   * view purges its rows.
   */
  @Test
  public void testViewWithNullMatcherHandledDuringCompaction() throws Exception {
    int surviving = runCoLocatedViewScenario(SecondView.NULL_MATCHER);
    assertEquals("A co-located view with a TTL but null ROW_KEY_MATCHER must be skipped, not abort "
      + "compaction; the legit view's TTL should still purge all rows", 0, surviving);
  }

  /**
   * Syntax-abort avoided (schema vector): a co-located view whose SCHEMA identifier contains a single
   * quote no longer corrupts the compaction-time TTL query. Because all three catalog identifier fields
   * (including {@code TABLE_SCHEM}) are now bound as parameters, there is no {@code ERROR 604} to abort
   * compaction, so the legit co-located view's TTL is applied and its rows are purged.
   */
  @Test
  public void testUnusualViewSchemaHandledDuringCompaction() throws Exception {
    int surviving = runCoLocatedViewScenario(SecondView.QUOTED_SCHEMA);
    assertEquals("A single-quote view SCHEMA must no longer abort compaction; the legit view's TTL "
      + "should still purge all rows", 0, surviving);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers (self-contained so this suite stands alone)
  // ---------------------------------------------------------------------------------------------

  /**
   * Which additional co-located view a {@link #runCoLocatedViewScenario} run plants alongside the
   * legit view: {@code NONE} (baseline), a view whose NAME or SCHEMA carries a single quote, or a
   * view with a TTL but a null/empty ROW_KEY_MATCHER (no {@code WHERE}).
   */
  private enum SecondView {
    NONE,
    QUOTED_NAME,
    QUOTED_SCHEMA,
    NULL_MATCHER
  }

  /**
   * Shared multi-tenant fix scenario behind the co-located-view tests. Creates a fresh multi-tenant
   * table and a legit tenant view (leading-PK {@code WHERE KP='AAA'} + short TTL, so its non-null
   * matcher purges its rows), optionally plants ONE additional co-located view that stresses the
   * compaction-time catalog lookup a different way, inserts {@link #NUM_ROWS} rows through the legit
   * view, advances the clock past the TTL, major-compacts the physical table, and returns the number
   * of surviving raw HBase rows. After the fix every variant leaves the legit view free to purge, so
   * all callers expect 0 survivors.
   *
   * @param plant which additional co-located view to create (see {@link SecondView})
   * @return count of surviving rows in the physical table after compaction
   */
  private int runCoLocatedViewScenario(SecondView plant) throws Exception {
    resetClock();

    String tableName = generateUniqueName();
    String legitView = "V" + generateUniqueName();
    String tenantId = nextTenantId();

    try (Connection global = DriverManager.getConnection(getUrl());
      Statement stmt = global.createStatement()) {
      stmt.execute(createTableDdl(tableName));
    }

    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();

    try (Connection tenant = DriverManager.getConnection(tenantUrl(tenantId))) {
      try (Statement stmt = tenant.createStatement()) {
        // Legit view: WHERE KP='AAA' pins a leading PK column -> non-null ROW_KEY_MATCHER (see
        // KEY_PREFIX); + TTL so it purges its rows.
        stmt.execute("CREATE VIEW " + legitView + " AS SELECT * FROM " + tableName + " WHERE KP = '"
          + KEY_PREFIX + "'");
        stmt.execute("ALTER VIEW " + legitView + " SET TTL='" + TTL_SECS + "'");
        // Optionally plant ONE additional co-located view that stresses the catalog lookup.
        switch (plant) {
          case QUOTED_NAME: {
            // Single quote in the view NAME; double-quoted so the quote rides into TABLE_NAME.
            String name = "Q'V" + generateUniqueName();
            stmt.execute("CREATE VIEW \"" + name + "\" AS SELECT * FROM " + tableName);
            break;
          }
          case QUOTED_SCHEMA: {
            // Single quote in the SCHEMA; double-quoted so the quote rides into TABLE_SCHEM.
            String schema = "S'" + generateUniqueName();
            String view = "V" + generateUniqueName();
            stmt.execute(
              "CREATE VIEW \"" + schema + "\".\"" + view + "\" AS SELECT * FROM " + tableName);
            break;
          }
          case NULL_MATCHER: {
            // TTL but no WHERE -> null/empty ROW_KEY_MATCHER (the NPE trigger the guard must skip).
            String view = "VNULL" + generateUniqueName();
            stmt.execute("CREATE VIEW " + view + " AS SELECT * FROM " + tableName);
            stmt.execute("ALTER VIEW " + view + " SET TTL='" + TTL_SECS + "'");
            break;
          }
          case NONE:
          default:
            break;
        }
      }
      try (Statement stmt = tenant.createStatement()) {
        upsertRowsViaView(stmt, legitView);
      }
      tenant.commit();
    }

    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (TTL_SECS * 1000L);
    majorCompactBaseTable(tableName, scnTimestamp);

    int surviving = rawRowCount(tableName, earliestTimestamp);
    LOGGER.info("Fix co-located-view scenario plant={} -> surviving rows={}", plant, surviving);
    return surviving;
  }

  /**
   * GLOBAL-view purge scenario for {@link #testGlobalViewNullTenantTtlStillPurges}. On a
   * non-multi-tenant base table (so a view created on a global connection is global / null-tenant and
   * its matcher applies at row-key offset 0), creates a global view {@code WHERE KP='AAA'} with a short
   * TTL, writes NUM_ROWS rows under {@code KP='AAA'} (covered by the matcher) and NUM_ROWS under
   * {@code KP='ZZZ'} (uncovered), advances the clock past the TTL, major-compacts, and returns the
   * surviving counts of each. Exercises {@code getGlobalViews -> getTTLInfo} with TENANT_ID/TABLE_SCHEM
   * bound as NULL (the fix's {@code setNull} path).
   *
   * @return {@code [survivingCovered(KP='AAA'), survivingUncovered(KP='ZZZ')]}
   */
  private int[] runGlobalViewPurgeScenario() throws Exception {
    resetClock();

    String tableName = generateUniqueName();
    String globalView = "GV" + generateUniqueName();

    long earliestTimestamp;
    try (Connection global = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = global.createStatement()) {
        stmt.execute(createNonTenantTableDdl(tableName));
      }
      earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();
      // GLOBAL view (null tenant): pins the leading PK KP -> non-null matcher = 'AAA' at offset 0.
      try (Statement stmt = global.createStatement()) {
        stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + tableName + " WHERE KP = '"
          + KEY_PREFIX + "'");
        stmt.execute("ALTER VIEW " + globalView + " SET TTL='" + TTL_SECS + "'");
      }
      // Covered (KP='AAA') + uncovered (KP='ZZZ') rows, written straight to the base table. View TTL
      // matching is by physical row-key prefix, independent of which view inserted the row.
      try (Statement stmt = global.createStatement()) {
        for (int i = 1; i <= NUM_ROWS; i++) {
          stmt.execute(String.format("UPSERT INTO %s (KP, ID, NUM) VALUES ('%s', 'R%04d', %d)",
            tableName, KEY_PREFIX, i, i));
          stmt.execute(String.format("UPSERT INTO %s (KP, ID, NUM) VALUES ('%s', 'R%04d', %d)",
            tableName, NON_MATCHING_KEY_PREFIX, i, i));
        }
      }
      global.commit();
    }

    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (TTL_SECS * 1000L);
    majorCompactBaseTable(tableName, scnTimestamp);

    int survivingCovered = rawRowCountWithKeyPrefix(tableName, earliestTimestamp, KEY_PREFIX);
    int survivingUncovered =
      rawRowCountWithKeyPrefix(tableName, earliestTimestamp, NON_MATCHING_KEY_PREFIX);
    LOGGER.info("Fix global-view scenario -> surviving covered(KP={})={} uncovered(KP={})={}",
      KEY_PREFIX, survivingCovered, NON_MATCHING_KEY_PREFIX, survivingUncovered);
    return new int[] { survivingCovered, survivingUncovered };
  }

  /**
   * Cross-table provenance scenario. Creates a donor table with a donor view (pinning WHERE => matcher,
   * plus a short TTL), a separate target table whose own view has NO TTL, inserts rows into the target,
   * optionally adds a second view on the target whose balanced name would append the donor's tuple to
   * the lookup, then major-compacts the target and returns the surviving row count. After the fix the
   * donor tuple is never honored, so both the second-view and control arms return {@code NUM_ROWS}.
   *
   * @param withSecondView whether to add the donor-tuple-appending view on the target
   * @return count of surviving rows in the target physical table after compaction
   */
  private int runProvenanceScenario(boolean withSecondView) throws Exception {
    // Start from a real (non-frozen) clock so the donor's ALTER SET TTL persists at wall-clock time.
    resetClock();

    String tenantId = nextTenantId(); // donor view, target rows, and second view all under this
    String donorTable = generateUniqueName();
    String donorSchema = "DSCH" + generateUniqueName();
    String donorView = "VDONOR" + generateUniqueName();
    String targetTable = generateUniqueName();
    String targetView = "VLEGIT" + generateUniqueName();

    try (Connection global = DriverManager.getConnection(getUrl());
      Statement stmt = global.createStatement()) {
      stmt.execute(createTableDdl(donorTable));
      stmt.execute(createTableDdl(targetTable));
    }

    long earliestTimestamp = EnvironmentEdgeManager.currentTimeMillis();

    // Donor view: WHERE KP='AAA' => non-null matcher [tenant][AAA]; short TTL that must not be applied.
    try (Connection tenant = DriverManager.getConnection(tenantUrl(tenantId));
      Statement stmt = tenant.createStatement()) {
      stmt.execute("CREATE VIEW " + donorSchema + "." + donorView + " AS SELECT * FROM " + donorTable
        + " WHERE KP = '" + KEY_PREFIX + "'");
      stmt.execute("ALTER VIEW " + donorSchema + "." + donorView + " SET TTL='" + TTL_SECS + "'");
    }

    // Read the donor's exact catalog identity to build the balanced second-view name below.
    String tid, schem, name;
    try (Connection tenant = DriverManager.getConnection(tenantUrl(tenantId));
      Statement stmt = tenant.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME FROM SYSTEM.CATALOG "
        + "WHERE TABLE_TYPE = 'v' AND TABLE_NAME = '" + donorView + "'")) {
      rs.next();
      tid = rs.getString("TENANT_ID");
      schem = rs.getString("TABLE_SCHEM");
      name = rs.getString("TABLE_NAME");
    }
    String tidLit = tid == null || tid.isEmpty() ? "NULL" : "'" + tid + "'";
    String schemLit = schem == null || schem.isEmpty() ? "NULL" : "'" + schem + "'";

    try (Connection tenant = DriverManager.getConnection(tenantUrl(tenantId))) {
      try (Statement stmt = tenant.createStatement()) {
        // Target's own view: same key range, but NO TTL -> its rows survive unless another TTL applies.
        stmt.execute("CREATE VIEW " + targetView + " AS SELECT * FROM " + targetTable + " WHERE KP = '"
          + KEY_PREFIX + "'");
        if (withSecondView) {
          // Second view on the TARGET; its balanced name would (before the fix) append the donor's
          // tuple to the getTTLInfo IN-list. With the fix the name is bound as data, so it cannot.
          String secondName = "X'),(" + tidLit + ", " + schemLit + ",'" + name;
          stmt.execute("CREATE VIEW \"" + secondName + "\" AS SELECT * FROM " + targetTable);
        }
      }
      try (Statement stmt = tenant.createStatement()) {
        upsertRowsViaView(stmt, targetView);
      }
      tenant.commit();
    }

    long scnTimestamp = EnvironmentEdgeManager.currentTimeMillis() + (TTL_SECS * 1000L);
    majorCompactBaseTable(targetTable, scnTimestamp);

    int surviving = rawRowCount(targetTable, earliestTimestamp);
    LOGGER.info("Fix provenance scenario withSecondView={} -> surviving target rows={}", withSecondView,
      surviving);
    return surviving;
  }

  /** Reset any frozen compaction clock from a prior scenario so DDL runs at wall-clock time. */
  private void resetClock() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  /** Upserts {@link #NUM_ROWS} rows (ID='R0001'.., NUM=i) through the given view. */
  private void upsertRowsViaView(Statement stmt, String viewName) throws SQLException {
    for (int i = 1; i <= NUM_ROWS; i++) {
      stmt.execute(String.format("UPSERT INTO %s (ID, NUM) VALUES ('R%04d', %d)", viewName, i, i));
    }
  }

  private static String createTableDdl(String tableName) {
    return "CREATE TABLE " + tableName + " (TENANT_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, "
      + "ID CHAR(5) NOT NULL, NUM INTEGER "
      + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, KP, ID)) "
      + "MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'";
  }

  /**
   * Non-multi-tenant table (no TENANT_ID column, no MULTI_TENANT): a view created on a global
   * connection is global / null-tenant, exercising {@code getGlobalViews -> getTTLInfo}'s null-tenant
   * bind. The leading PK is KP, so a {@code WHERE KP=...} matcher applies at row-key offset 0.
   */
  private static String createNonTenantTableDdl(String tableName) {
    return "CREATE TABLE " + tableName + " (KP CHAR(3) NOT NULL, ID CHAR(5) NOT NULL, NUM INTEGER "
      + "CONSTRAINT PK PRIMARY KEY (KP, ID)) COLUMN_ENCODED_BYTES=0, DEFAULT_COLUMN_FAMILY='0'";
  }

  private String nextTenantId() {
    // 15-char CHAR tenant id, matching the base table's TENANT_ID column width.
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

  /** Counts physical rows whose row key starts with the given ASCII prefix (raw HBase scan). */
  private int rawRowCountWithKeyPrefix(String tableName, long minTimestamp, String keyPrefix)
    throws IOException, SQLException {
    byte[] hbaseTableName = Bytes.toBytes(tableName);
    byte[] prefix = Bytes.toBytes(keyPrefix);
    try (Table tbl = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
      .getTable(hbaseTableName)) {
      Scan scan = new Scan();
      scan.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
      try (ResultScanner scanner = tbl.getScanner(scan)) {
        int numRows = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          if (Bytes.startsWith(result.getRow(), prefix)) {
            numRows++;
          }
        }
        return numRows;
      }
    }
  }
}
