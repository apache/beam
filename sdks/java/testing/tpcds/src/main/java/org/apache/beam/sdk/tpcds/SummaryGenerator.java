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
package org.apache.beam.sdk.tpcds;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate the tpcds queries execution summary on the command line after finishing all jobs.
 */
public class SummaryGenerator {
    private static final int PADDING_SIZE = 2;
    private static final String NEW_LINE = "\n";
    private static final String TABLE_JOINT_SYMBOL = "+";
    private static final String TABLE_V_SPLIT_SYMBOL = "|";
    private static final String TABLE_H_SPLIT_SYMBOL = "-";

    public static String generateTable(List<String> headersList, List<List<String>> rowsList,int... overRiddenHeaderHeight) {
        StringBuilder stringBuilder = new StringBuilder();

        int rowHeight = overRiddenHeaderHeight.length > 0 ? overRiddenHeaderHeight[0] : 1;

        Map<Integer,Integer> columnMaxWidthMapping = getMaximumWidthofTable(headersList, rowsList);

        stringBuilder.append(NEW_LINE);
        stringBuilder.append(NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(NEW_LINE);

        for (int headerIndex = 0; headerIndex < headersList.size(); headerIndex++) {
            fillCell(stringBuilder, headersList.get(headerIndex), headerIndex, columnMaxWidthMapping);
        }

        stringBuilder.append(NEW_LINE);

        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);

        for (List<String> row : rowsList) {
            for (int i = 0; i < rowHeight; i++) {
                stringBuilder.append(NEW_LINE);
            }
            for (int cellIndex = 0; cellIndex < row.size(); cellIndex++) {
                fillCell(stringBuilder, row.get(cellIndex), cellIndex, columnMaxWidthMapping);
            }
        }

        stringBuilder.append(NEW_LINE);
        createRowLine(stringBuilder, headersList.size(), columnMaxWidthMapping);
        stringBuilder.append(NEW_LINE);
        stringBuilder.append(NEW_LINE);

        return stringBuilder.toString();
    }

    private static void fillSpace(StringBuilder stringBuilder, int length) {
        for (int i = 0; i < length; i++) {
            stringBuilder.append(" ");
        }
    }

    /** Add a rowLine at the beginning, the middle between headersList and rowLists, the end of the summary table. */
    private static void createRowLine(StringBuilder stringBuilder,int headersListSize, Map<Integer,Integer> columnMaxWidthMapping) {
        for (int i = 0; i < headersListSize; i++) {
            if(i == 0) {
                stringBuilder.append(TABLE_JOINT_SYMBOL);
            }

            for (int j = 0; j < columnMaxWidthMapping.get(i) + PADDING_SIZE * 2 ; j++) {
                stringBuilder.append(TABLE_H_SPLIT_SYMBOL);
            }
            stringBuilder.append(TABLE_JOINT_SYMBOL);
        }
    }

    /** Get the width of the summary table. */
    private static Map<Integer,Integer> getMaximumWidthofTable(List<String> headersList, List<List<String>> rowsList) {
        Map<Integer,Integer> columnMaxWidthMapping = new HashMap<>();

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
            columnMaxWidthMapping.put(columnIndex, 0);
        }

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
            if(headersList.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex)) {
                columnMaxWidthMapping.put(columnIndex, headersList.get(columnIndex).length());
            }
        }

        for (List<String> row : rowsList) {
            for (int columnIndex = 0; columnIndex < row.size(); columnIndex++) {
                if(row.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex)) {
                    columnMaxWidthMapping.put(columnIndex, row.get(columnIndex).length());
                }
            }
        }

        for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
            if(columnMaxWidthMapping.get(columnIndex) % 2 != 0) {
                columnMaxWidthMapping.put(columnIndex, columnMaxWidthMapping.get(columnIndex) + 1);
            }
        }

        return columnMaxWidthMapping;
    }

    private static int getOptimumCellPadding(int cellIndex,int datalength,Map<Integer,Integer> columnMaxWidthMapping,int cellPaddingSize) {
        if(datalength % 2 != 0) {
            datalength++;
        }

        if(datalength < columnMaxWidthMapping.get(cellIndex)) {
            cellPaddingSize = cellPaddingSize + (columnMaxWidthMapping.get(cellIndex) - datalength) / 2;
        }

        return cellPaddingSize;
    }

    /** Use space to fill a single cell with optimum cell padding size. */
    private static void fillCell(StringBuilder stringBuilder,String cell,int cellIndex,Map<Integer,Integer> columnMaxWidthMapping) {

        int cellPaddingSize = getOptimumCellPadding(cellIndex, cell.length(), columnMaxWidthMapping, PADDING_SIZE);

        if(cellIndex == 0) {
            stringBuilder.append(TABLE_V_SPLIT_SYMBOL);
        }

        fillSpace(stringBuilder, cellPaddingSize);
        stringBuilder.append(cell);
        if(cell.length() % 2 != 0) {
            stringBuilder.append(" ");
        }

        fillSpace(stringBuilder, cellPaddingSize);

        stringBuilder.append(TABLE_V_SPLIT_SYMBOL);
    }
}
