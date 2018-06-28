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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** A {@link RelNode} that can also give a {@link PTransform} that implements the expression. */
public interface BeamRelNode extends RelNode {

  default List<RelNode> getPCollectionInputs() {
    return getInputs();
  };

  PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform();

  /** Perform a DFS(Depth-First-Search) to find the PipelineOptions config. */
  default Map<String, String> getPipelineOptions() {
    Map<String, String> options = null;
    for (RelNode input : getInputs()) {
      Map<String, String> inputOptions = ((BeamRelNode) input).getPipelineOptions();
      assert inputOptions != null;
      assert options == null || options == inputOptions;
      options = inputOptions;
    }
    return options;
  }

  default double estimateRowSize(RelDataType rowType) {
    List<RelDataTypeField> fieldList = rowType.getFieldList();

    double score =
        fieldList
            .stream()
            .mapToDouble(typeField -> estimateDataTypeSize(typeField.getType()))
            .sum();

    return score;
  }

  default double estimateDataTypeSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
        return 1;
      case SMALLINT:
        return 2;
      case INTEGER:
        return 4;
      case BIGINT:
        return 8;
      case BOOLEAN:
        return 1;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case VARCHAR:
        return 12;
      case CHAR:
        return 1;
      case DECIMAL:
        return 12;
        //      case typeName if SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName) : 8
        //      case typeName if SqlTypeName.DAY_INTERVAL_TYPES.contains(typeName) : 4
      case TIME:
      case TIMESTAMP:
      case DATE:
        return 12;
      case ROW:
        return estimateRowSize(type);
        // 16 is an arbitrary estimate
      case ARRAY:
        return estimateDataTypeSize(type.getComponentType()) * 16;
      case MAP:
      case MULTISET:
        // 16 is an arbitrary estimate
        return (estimateDataTypeSize(type.getKeyType()) + estimateDataTypeSize(type.getValueType()))
            * 16;
      case ANY:
        return 128; // 128 is an arbitrary estimate
      default:
        throw new UnsupportedOperationException("Unsupported data type");
    }
  }
}
