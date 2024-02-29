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

package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.util.StringUtils;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.DescVarLengthFastByteComparisons;
import org.apache.phoenix.schema.PTable;

import static org.apache.phoenix.coprocessor.GlobalIndexRegionScanner.adjustScanFilter;

public class CDCUtil {
    public static final String CDC_INDEX_PREFIX = "__CDC__";
    public static final String CDC_INDEX_TYPE_LOCAL = "L";

    /**
     * Make a set of CDC change scope enums from the given string containing comma separated scope
     * names.
     *
     * @param includeScopes Comma-separated scope names.
     * @return the set of enums, which can be empty if the string is empty or has no valid names.
     */
    public static Set<PTable.CDCChangeScope> makeChangeScopeEnumsFromString(String includeScopes)
            throws SQLException {
        Set<PTable.CDCChangeScope> cdcChangeScopes = new HashSet<>();
        if (includeScopes != null) {
            StringTokenizer st  = new StringTokenizer(includeScopes, ",");
            while (st.hasMoreTokens()) {
                String tok = st.nextToken();
                try {
                    cdcChangeScopes.add(PTable.CDCChangeScope.valueOf(tok.trim().toUpperCase()));
                }
                catch (IllegalArgumentException e) {
                    throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.UNKNOWN_INCLUDE_CHANGE_SCOPE).setCdcChangeScope(
                                    tok).build().buildException();
                }
            }
        }
        return cdcChangeScopes;
    }

    /**
     * Make a string of comma-separated scope names from the specified set of enums.
     *
     * @param includeScopes Set of scope enums
     * @return the comma-separated string of scopes, which can be an empty string in case the set is empty.
     */
    public static String makeChangeScopeStringFromEnums(Set<PTable.CDCChangeScope> includeScopes) {
        String cdcChangeScopes = null;
        if (includeScopes != null) {
            Iterable<String> tmpStream = () -> includeScopes.stream().sorted()
                    .map(s -> s.name()).iterator();
            cdcChangeScopes = StringUtils.join(",", tmpStream);
        }
        return cdcChangeScopes;
    }

    public static String getCDCIndexName(String cdcName) {
        return CDC_INDEX_PREFIX + SchemaUtil.getTableNameFromFullName(cdcName.toUpperCase());
    }

    public static boolean isCDCIndex(String indexName) {
        return indexName.startsWith(CDC_INDEX_PREFIX);
    }

    public static boolean isCDCIndex(PTable indexTable) {
        return isCDCIndex(indexTable.getTableName().getString());
    }

    public static Scan initForRawScan(Scan scan) {
        scan.setRaw(true);
        scan.readAllVersions();
        scan.setCacheBlocks(false);
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        if (! familyMap.isEmpty()) {
            familyMap.keySet().stream().forEach(fQual -> {
                if (familyMap.get(fQual) != null) {
                    familyMap.get(fQual).clear();
                }
            });
        }
        return scan;
    }

    public static int compareCellFamilyAndQualifier(byte[] columnFamily1,
                                                     byte[] columnQual1,
                                                     byte[] columnFamily2,
                                                     byte[] columnQual2) {
        int familyNameComparison = DescVarLengthFastByteComparisons.compareTo(columnFamily1,
                0, columnFamily1.length, columnFamily2, 0, columnFamily2.length);
        if (familyNameComparison != 0) {
            return familyNameComparison;
        }
        return DescVarLengthFastByteComparisons.compareTo(columnQual1,
                0, columnQual1.length, columnQual2, 0, columnQual2.length);
    }
}
