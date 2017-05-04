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
package org.apache.beam.dsls.sql.transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.util.ImmutableBitSet;
import org.joda.time.Instant;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform GROUP-BY operation.
 */
public class BeamAggregationTransform implements Serializable{
  /**
   * Merge KV to single record.
   */
  public static class MergeAggregationRecord extends DoFn<KV<BeamSQLRow, Long>, BeamSQLRow> {
    private BeamSQLRecordType outRecordType;
    private String aggFieldName;

    public MergeAggregationRecord(BeamSQLRecordType outRecordType, String aggFieldName) {
      this.outRecordType = outRecordType;
      this.aggFieldName = aggFieldName;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      BeamSQLRow outRecord = new BeamSQLRow(outRecordType);
      outRecord.updateWindowRange(c.element().getKey(), window);

      KV<BeamSQLRow, Long> kvRecord = c.element();
      for (String f : kvRecord.getKey().getDataType().getFieldsName()) {
        outRecord.addField(f, kvRecord.getKey().getFieldValue(f));
      }
      outRecord.addField(aggFieldName, kvRecord.getValue());

//      if (c.pane().isLast()) {
      c.output(outRecord);
//      }
    }
  }

  /**
   * extract group-by fields.
   */
  public static class AggregationGroupByKeyFn
      implements SerializableFunction<BeamSQLRow, BeamSQLRow> {
    private List<Integer> groupByKeys;

    public AggregationGroupByKeyFn(int windowFieldIdx, ImmutableBitSet groupSet) {
      this.groupByKeys = new ArrayList<>();
      for (int i : groupSet.asList()) {
        if (i != windowFieldIdx) {
          groupByKeys.add(i);
        }
      }
    }

    @Override
    public BeamSQLRow apply(BeamSQLRow input) {
      BeamSQLRecordType typeOfKey = exTypeOfKeyRecord(input.getDataType());
      BeamSQLRow keyOfRecord = new BeamSQLRow(typeOfKey);
      keyOfRecord.updateWindowRange(input, null);

      for (int idx = 0; idx < groupByKeys.size(); ++idx) {
        keyOfRecord.addField(idx, input.getFieldValue(groupByKeys.get(idx)));
      }
      return keyOfRecord;
    }

    private BeamSQLRecordType exTypeOfKeyRecord(BeamSQLRecordType dataType) {
      BeamSQLRecordType typeOfKey = new BeamSQLRecordType();
      for (int idx : groupByKeys) {
        typeOfKey.addField(dataType.getFieldsName().get(idx), dataType.getFieldsType().get(idx));
      }
      return typeOfKey;
    }

  }

  /**
   * Assign event timestamp.
   */
  public static class WindowTimestampFn implements SerializableFunction<BeamSQLRow, Instant> {
    private int windowFieldIdx = -1;

    public WindowTimestampFn(int windowFieldIdx) {
      super();
      this.windowFieldIdx = windowFieldIdx;
    }

    @Override
    public Instant apply(BeamSQLRow input) {
      return new Instant(input.getDate(windowFieldIdx).getTime());
    }
  }

}
