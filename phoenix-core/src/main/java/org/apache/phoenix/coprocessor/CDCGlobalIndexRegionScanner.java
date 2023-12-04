package org.apache.phoenix.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_DATA_TABLE_NAME;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PHYSICAL_DATA_TABLE_NAME;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);
    protected final Table indexHTable;

    public CDCGlobalIndexRegionScanner (final RegionScanner innerScanner,
                                        final Region region,
                                        final Scan scan,
                                        final RegionCoprocessorEnvironment env,
                                        final Scan dataTableScan,
                                        final TupleProjector tupleProjector,
                                        final IndexMaintainer indexMaintainer,
                                        final byte[][] viewConstants,
                                        final ImmutableBytesWritable ptr,
                                        final long pageSizeMs,
                                        final long queryLimit)
            throws IOException {

        super(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
                viewConstants, ptr, pageSizeMs, queryLimit);

        byte[] indexTableName = CDCUtil.getCDCIndexName(scan.getAttribute(CDC_DATA_TABLE_NAME).toString()).getBytes();
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr("N000002".getBytes()));


    }
}
