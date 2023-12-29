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
package org.apache.phoenix.coprocessor;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_JSON_COL_QUALIFIER;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.DATA_COL_QUALIFIER_TO_NAME_MAP;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.DATA_COL_QUALIFIER_TO_TYPE_MAP;

public class CDCGlobalIndexRegionScanner extends UncoveredGlobalIndexRegionScanner {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CDCGlobalIndexRegionScanner.class);

    private Map<ImmutableBytesPtr, String> dataColQualNameMap;
    private Map<ImmutableBytesPtr, PDataType> dataColQualTypeMap;
    // Map<dataRowKey: Map<TS: Map<qualifier: Cell>>>
    private Map<ImmutableBytesPtr, Map<Long, Map<ImmutableBytesPtr, Cell>>> dataRowChanges =
            new HashMap<>();

    private final static String EVENT_TYPE = "event_type";
    private final static String IMAGE = "image";
    private final static String PRE_IMAGE = "pre";
    private final static String POST_IMAGE = "post";
    private final static String CHANGE_IMAGE = "change";
    private final static String UPSERT_EVENT_TYPE = "upsert";
    private final static String DELETE_EVENT_TYPE = "delete";
    private final static String DELETE_MARKER_COLUMN_QUALIFIER = "";

    public CDCGlobalIndexRegionScanner(final RegionScanner innerScanner,
                                       final Region region,
                                       final Scan scan,
                                       final RegionCoprocessorEnvironment env,
                                       final Scan dataTableScan,
                                       final TupleProjector tupleProjector,
                                       final IndexMaintainer indexMaintainer,
                                       final byte[][] viewConstants,
                                       final ImmutableBytesWritable ptr,
                                       final long pageSizeMs,
                                       final long queryLimit) throws IOException {
        super(innerScanner, region, scan, env, dataTableScan, tupleProjector, indexMaintainer,
                viewConstants, ptr, pageSizeMs, queryLimit);
        CDCUtil.initForRawScan(dataTableScan);
        dataColQualNameMap = ScanUtil.deserializeColumnQualifierToNameMap(
                scan.getAttribute(DATA_COL_QUALIFIER_TO_NAME_MAP));
        dataColQualTypeMap = ScanUtil.deserializeColumnQualifierToTypeMap(
                scan.getAttribute(DATA_COL_QUALIFIER_TO_TYPE_MAP));
    }

    @Override
    protected Scan prepareDataTableScan(Collection<byte[]> dataRowKeys) throws IOException {
        return CDCUtil.initForRawScan(prepareDataTableScan(dataRowKeys, true));
    }

    protected boolean getNextCoveredIndexRow(List<Cell> result) throws IOException {
        if (indexRowIterator.hasNext()) {
            List<Cell> indexRow = indexRowIterator.next();
            for (Cell c: indexRow) {
                if (c.getType() == Cell.Type.Put) {
                    result.add(c);
                }
            }
            try {
                byte[] indexRowKey = indexRow.get(0).getRowArray();
                Long indexRowTs = result.get(0).getTimestamp();
                ImmutableBytesPtr dataRowKey = new ImmutableBytesPtr(
                        indexToDataRowKeyMap.get(indexRowKey));
                Result dataRow = dataRows.get(dataRowKey);
                Long postIndexRowTs = null;
                List<Long> sortedTimestamps = null;
                if (dataRow != null) {
                    Map<Long, Map<ImmutableBytesPtr, Cell>> changeTimeline = dataRowChanges.get(
                            dataRowKey);
                    Map<ImmutableBytesPtr, Map<Long, Cell>> changeTimelinePerColumnQualifier = new HashMap<>();
                    if (changeTimeline == null) {
                        List<Cell> resultCells = Arrays.asList(dataRow.rawCells());
                        Collections.sort(resultCells, CellComparator.getInstance().reversed());
                        List<Cell> deleteMarkers = new ArrayList<>();
                        List<List<Cell>> columns = new LinkedList<>();
                        Cell currentColumnCell = null;
                        Pair<byte[], byte[]> emptyKV = EncodedColumnsUtil.getEmptyKeyValueInfo(
                                EncodedColumnsUtil.getQualifierEncodingScheme(scan));
                        List<Cell> currentColumn = null;
                        Set<Long> uniqueTimeStamps = new HashSet<>();
                        // TODO: From CompactionScanner.formColumns(), see if this can be refactored.
                        for (Cell cell : resultCells) {
                            uniqueTimeStamps.add(cell.getTimestamp());
                            if (cell.getType() != Cell.Type.Put) {
                                deleteMarkers.add(cell);
                            }
                            if (CellUtil.matchingColumn(cell, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                                    emptyKV.getFirst())) {
                                continue;
                            }
                            if (currentColumnCell == null) {
                                currentColumn = new LinkedList<>();
                                currentColumnCell = cell;
                                currentColumn.add(cell);
                            } else if (!CellUtil.matchingColumn(cell, currentColumnCell)) {
                                columns.add(currentColumn);
                                currentColumn = new LinkedList<>();
                                currentColumnCell = cell;
                                currentColumn.add(cell);
                            } else {
                                currentColumn.add(cell);
                            }
                        }
                        if (currentColumn != null) {
                            columns.add(currentColumn);
                        }
                        sortedTimestamps = uniqueTimeStamps.stream().sorted().collect(
                                Collectors.toList());
                        // FIXME: Does this need to be Concurrent?
                        int[] columnPointers = new int[columns.size()];
                        changeTimeline = new TreeMap<>();
                        dataRowChanges.put(dataRowKey, changeTimeline);
                        for (int tsIndex = 0; tsIndex < sortedTimestamps.size(); tsIndex++) {
                            if (tsIndex == indexRowTs && tsIndex < (sortedTimestamps.size() - 1)) {
                                postIndexRowTs = sortedTimestamps.get(tsIndex + 1);
                            }
                            Map<ImmutableBytesPtr, Cell> rollingRow = new HashMap<>();
                            for (int i = 0; i < columns.size(); ++i) {
                                if (columns.get(i).size() == columnPointers[i]) {
                                    continue;
                                }
                                Cell cell = columns.get(i).get(columnPointers[i]);
                                if (cell.getTimestamp() == sortedTimestamps.get(tsIndex)) {
                                    rollingRow.put(new ImmutableBytesPtr(cell.getQualifierArray()), cell);
                                    if (changeTimelinePerColumnQualifier.get(new ImmutableBytesPtr(cell.getQualifierArray())) == null) {
                                        changeTimelinePerColumnQualifier.put(new ImmutableBytesPtr(cell.getQualifierArray()), new TreeMap<>());
                                    }
                                    changeTimelinePerColumnQualifier.get(new ImmutableBytesPtr(cell.getQualifierArray())).put(sortedTimestamps.get(tsIndex), cell);
                                    ++columnPointers[i];
                                }
                            }
                            Map<ImmutableBytesPtr, Cell> rowOfCells = new HashMap();
                            rowOfCells.putAll(rollingRow);
                            changeTimeline.put(sortedTimestamps.get(tsIndex), rowOfCells);
                        }
                    }

                    Map<ImmutableBytesPtr, Cell> mapOfCells = changeTimeline.get(indexRowTs);
                    if (mapOfCells != null) {
                        Map <String, Object> rowValueMap = new HashMap<>(mapOfCells.size());
                        for (Map.Entry<ImmutableBytesPtr, Cell> entry: mapOfCells.entrySet()) {
                            rowValueMap.put(EVENT_TYPE, getEventType(changeTimeline, postIndexRowTs));

                            Map<String, Map<String, Object>> currentIndexImages =
                                    getRowImages(changeTimelinePerColumnQualifier, indexRowTs);
                            rowValueMap.put(PRE_IMAGE, currentIndexImages.get(PRE_IMAGE));
                            rowValueMap.put(POST_IMAGE, currentIndexImages.get(POST_IMAGE));
                        }
                        Cell firstCell = result.get(0);
                        byte[] value =
                                new Gson().toJson(rowValueMap).getBytes(StandardCharsets.UTF_8);
                        CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
                        dataRow = Result.create(Arrays.asList(builder.
                                setRow(indexToDataRowKeyMap.get(indexRowKey)).
                                setFamily(firstCell.getFamilyArray()).
                                setQualifier(scan.getAttribute((CDC_JSON_COL_QUALIFIER))).
                                setTimestamp(indexRow.get(0).getTimestamp()).
                                setValue(value).
                                setType(Cell.Type.Put).
                                build()));
                    }
                }
                if (dataRow != null && tupleProjector != null) {
                    IndexUtil.addTupleAsOneCell(result, new ResultTuple(dataRow),
                            tupleProjector, ptr);
                }
                return true;
            } catch (Throwable e) {
                LOGGER.error("Exception in UncoveredIndexRegionScanner for region "
                        + region.getRegionInfo().getRegionNameAsString(), e);
                throw e;
            }
        }
        return false;
    }

    private Object getEventType(Map<Long, Map<ImmutableBytesPtr, Cell>> changeTimeline,
                              Long postIndexRowTs) throws IOException {
        Object eventType = UPSERT_EVENT_TYPE;
        if (postIndexRowTs != null) {
            if (changeTimeline.get(postIndexRowTs).get(DELETE_MARKER_COLUMN_QUALIFIER) != null) {
                eventType = DELETE_EVENT_TYPE;
            }
        }
        return eventType;
    }

    private Map<String, Map<String, Object>> getRowImages(
            Map<ImmutableBytesPtr, Map<Long, Cell>> changeTimelinePerColQualifier,
            Long indexRowTs) {
        Map<String, Map<String, Object>> images = new HashMap<>();
        // Prepare PreImage
        Map<String, Object> preImage = new HashMap<>();
        for (Map.Entry<ImmutableBytesPtr, Map<Long, Cell>> colQualifierCells : changeTimelinePerColQualifier.entrySet()) {
            Object colVal = null;
            if(dataColQualNameMap.get(colQualifierCells.getKey()) != null) {
                for (Map.Entry<Long, Cell> columnCell : colQualifierCells.getValue().entrySet()) {
                    if (columnCell.getKey() >= indexRowTs) {
                        break;
                    }
                    colVal = dataColQualTypeMap.get(colQualifierCells.getKey()).toObject(
                            columnCell.getValue().getValueArray());
                }
                String colName = dataColQualNameMap.get(colQualifierCells.getKey());
                preImage.put(colName, colVal);
            }
        }
        images.put(PRE_IMAGE, preImage);

        // Prepare PostImage
        Map<String, Object> postImage = new HashMap<>();
        for (Map.Entry<ImmutableBytesPtr, Map<Long, Cell>> colQualifierCells : changeTimelinePerColQualifier.entrySet()) {
            Object colVal = null;
            if(dataColQualNameMap.get(colQualifierCells.getKey()) != null) {
                for (Map.Entry<Long, Cell> columnCell : colQualifierCells.getValue().entrySet()) {
                    if (columnCell.getKey() > indexRowTs) {
                        break;
                    }
                    colVal = dataColQualTypeMap.get(colQualifierCells.getKey()).toObject(
                            columnCell.getValue().getValueArray());
                }
                String colName = dataColQualNameMap.get(colQualifierCells.getKey());
                postImage.put(colName, colVal);
            }
        }
        images.put(POST_IMAGE, postImage);

//        for (Map.Entry<String, Object> colQualifiers : postImage.entrySet()) {
//            if (colQualifiers.)
//        }

        return images;
    }

//    private Map<String, Object> getChangeRowImage (Map<String, Object> preImageRow,
//                                                   Map<String, Object> currentImageRow) {
//
//        Map<String, Object> image = new HashMap<>();
//
//    }
}
