/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.timeseries.utils;

import com.google.protobuf.util.Timestamps;
import java.math.BigDecimal;

import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.annotations.Experimental;
import org.tensorflow.example.*;

/** Utility functions for TSData. */
@Experimental
public class TSDatas {

  public static String getStringValue(TimeSeriesData.Data data) {

    switch (data.getDataPointCase()) {
      case INT_VAL:
        {
          return String.valueOf(data.getIntVal());
        }
      case DOUBLE_VAL:
        {
          return String.valueOf(data.getDoubleVal());
        }
      case LONG_VAL:
        {
          return String.valueOf(data.getLongVal());
        }
      default:
        return "";
    }
  }

  public static TimeSeriesData.Data sumData(TimeSeriesData.Data a, TimeSeriesData.Data b) {

    TimeSeriesData.Data.Builder data = TimeSeriesData.Data.newBuilder();

    switch (getCaseFromTwoTSDataValues(a, b)) {
      case INT_VAL:
        {
          Integer sum = a.getIntVal() + b.getIntVal();
          return data.setIntVal(sum).build();
        }
      case DOUBLE_VAL:
        {
          Double sum = a.getDoubleVal() + b.getDoubleVal();
          return data.setDoubleVal(sum).build();
        }
      case LONG_VAL:
        {
          Long sum = a.getLongVal() + b.getLongVal();
          return data.setLongVal(sum).build();
        }
      case FLOAT_VAL:
        {
          Float sum = a.getFloatVal() + b.getFloatVal();
          return data.setFloatVal(sum).build();
        }
      default:
        {
          // Not implemented
          return null;
        }
    }
  }

  public static TimeSeriesData.Data.DataPointCase getCaseFromTwoTSDataValues(
      TimeSeriesData.Data a, TimeSeriesData.Data b) {

    // a or b may not have had the value set, so we need to return the first that has

    TimeSeriesData.Data.DataPointCase dataPointCaseA = a.getDataPointCase();
    TimeSeriesData.Data.DataPointCase dataPointCaseB = b.getDataPointCase();
    TimeSeriesData.Data.DataPointCase dataPointCase;

    if (dataPointCaseA == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET
        && dataPointCaseB == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      dataPointCase = TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET;
    } else if (dataPointCaseA != TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      dataPointCase = dataPointCaseA;
    } else {
      dataPointCase = dataPointCaseB;
    }

    return dataPointCase;
  }

  // Return minimum that has been set
  public static TimeSeriesData.Data findMinDataIfSet(TimeSeriesData.Data a, TimeSeriesData.Data b) {

    // If neither A or B have had the value set then return A
    if (a.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET
        && b.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    // If A is not set then return B
    if (a.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return b;
    }

    // If B is not set then return A
    if (b.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    switch (getCaseFromTwoTSDataValues(a, b)) {
      case INT_VAL:
        {
          return (a.getIntVal() < b.getIntVal()) ? a : b;
        }
      case DOUBLE_VAL:
        {
          return (a.getDoubleVal() < b.getDoubleVal()) ? a : b;
        }
      case LONG_VAL:
        {
          return (a.getLongVal() < b.getLongVal()) ? a : b;
        }
      case FLOAT_VAL:
        return (a.getFloatVal() < b.getFloatVal()) ? a : b;
      default:
        return a;
    }
  }

  public static TimeSeriesData.Data findMaxData(TimeSeriesData.Data a, TimeSeriesData.Data b) {

    // If neither A or B have had the value set then return A
    if (a.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET
        && b.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    // If A is not set then return B
    if (a.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return b;
    }

    // If B is not set then return A
    if (b.getDataPointCase() == TimeSeriesData.Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    switch (getCaseFromTwoTSDataValues(a, b)) {
      case DOUBLE_VAL:
        {
          return (a.getDoubleVal() > b.getDoubleVal()) ? a : b;
        }
      case LONG_VAL:
        {
          return (a.getLongVal() > b.getLongVal()) ? a : b;
        }
      case INT_VAL:
        {
          return (a.getIntVal() > b.getIntVal()) ? a : b;
        }
      case FLOAT_VAL:
        {
          return (a.getFloatVal() > b.getFloatVal()) ? a : b;
        }
      default:
        return a;
    }
  }

  public static Feature getFeatureFromTSDataPoint(TimeSeriesData.Data data) {
    Feature.Builder feature = Feature.newBuilder();

    switch (data.getDataPointCase()) {
      case DOUBLE_VAL:
        {
          feature.setFloatList(
              FloatList.newBuilder()
                  .addValue(BigDecimal.valueOf(data.getDoubleVal()).floatValue()));
          break;
        }
      case LONG_VAL:
        {
          feature.setFloatList(FloatList.newBuilder().addValue(data.getLongVal()));
          break;
        }
      case INT_VAL:
        {
          feature.setInt64List(Int64List.newBuilder().addValue(data.getIntVal()));
          break;
        }
      default:
        break;
    }
    return feature.build();
  }

  public static TimeSeriesData.TSDataPoint findMinTimeStamp(
      TimeSeriesData.TSDataPoint a, TimeSeriesData.TSDataPoint b) {

    // Check if either timestamp is zero, if yes then return the other value
    if (Timestamps.toMillis(a.getTimestamp()) == 0 && Timestamps.toMillis(b.getTimestamp()) > 0) {
      return b;
    }

    if (Timestamps.toMillis(b.getTimestamp()) == 0) {
      return a;
    }

    return (Timestamps.comparator().compare(a.getTimestamp(), b.getTimestamp()) < 0) ? a : b;
  }

  public static TimeSeriesData.TSDataPoint findMaxTimeStamp(
      TimeSeriesData.TSDataPoint a, TimeSeriesData.TSDataPoint b) {

    // Check if either timestamp is zero, if yes then return the other value
    if (Timestamps.toMillis(a.getTimestamp()) == 0 && Timestamps.toMillis(b.getTimestamp()) > 0) {
      return b;
    }

    if (Timestamps.toMillis(b.getTimestamp()) == 0) {
      return a;
    }

    return (Timestamps.comparator().compare(a.getTimestamp(), b.getTimestamp()) > 0) ? a : b;
  }

  public static <T extends Number> TimeSeriesData.Data createData(T data) {

    if (data instanceof Double) {
      return TimeSeriesData.Data.newBuilder().setDoubleVal((Double) data).build();
    }

    if (data instanceof Integer) {
      return TimeSeriesData.Data.newBuilder().setIntVal((Integer) data).build();
    }

    if (data instanceof Float) {
      return TimeSeriesData.Data.newBuilder().setFloatVal((Float) data).build();
    }

    throw new IllegalArgumentException("Must be Double, Integer or Float");
  }
}
