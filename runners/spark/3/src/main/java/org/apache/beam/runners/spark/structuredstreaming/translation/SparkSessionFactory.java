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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SideInputValues;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.BitSetCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.DequeCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.SnappyCoder;
import org.apache.beam.sdk.coders.SortedMapCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.coders.TimestampPrefixingWindowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSessionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSessionFactory.class);

  /**
   * Gets active {@link SparkSession} or creates one using {@link
   * SparkStructuredStreamingPipelineOptions}.
   */
  public static SparkSession getOrCreateSession(SparkStructuredStreamingPipelineOptions options) {
    if (options.getUseActiveSparkSession()) {
      return SparkSession.active();
    }
    return sessionBuilder(options.getSparkMaster(), options.getAppName(), options.getFilesToStage())
        .getOrCreate();
  }

  /** Creates Spark session builder with some optimizations for local mode, e.g. in tests. */
  public static SparkSession.Builder sessionBuilder(String master) {
    return sessionBuilder(master, null, null);
  }

  private static SparkSession.Builder sessionBuilder(
      String master, @Nullable String appName, @Nullable List<String> jars) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(master);
    if (appName != null) {
      sparkConf.setAppName(appName);
    }
    if (jars != null && !jars.isEmpty()) {
      sparkConf.setJars(jars.toArray(new String[0]));
    }

    // Set to 'org.apache.spark.serializer.JavaSerializer' via system property to disable Kryo
    String serializer = sparkConf.get("spark.serializer", KryoSerializer.class.getName());
    if (serializer.equals(KryoSerializer.class.getName())) {
      // Set to 'false' via system property to disable usage of Kryo unsafe
      boolean unsafe = sparkConf.getBoolean("spark.kryo.unsafe", true);
      sparkConf.set("spark.serializer", serializer);
      sparkConf.set("spark.kryo.unsafe", Boolean.toString(unsafe));
      sparkConf.set("spark.kryo.registrator", SparkKryoRegistrator.class.getName());
      LOG.info("Configured `spark.serializer` to use KryoSerializer [unsafe={}]", unsafe);
    }

    // By default, Spark defines 200 as a number of sql partitions. This seems too much for local
    // mode, so try to align with value of "sparkMaster" option in this case.
    // We should not overwrite this value (or any user-defined spark configuration value) if the
    // user has already configured it.
    if (master != null
        && !master.equals("local[*]")
        && master.startsWith("local[")
        && System.getProperty("spark.sql.shuffle.partitions") == null) {
      int numPartitions =
          Integer.parseInt(master.substring("local[".length(), master.length() - 1));
      if (numPartitions > 0) {
        sparkConf.set("spark.sql.shuffle.partitions", String.valueOf(numPartitions));
      }
    }
    return SparkSession.builder().config(sparkConf);
  }

  /**
   * {@link KryoRegistrator} for Spark to serialize broadcast variables used for side-inputs.
   *
   * <p>Note, this registrator must be public to be accessible for Kryo.
   *
   * @see SideInputValues
   */
  public static class SparkKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
      kryo.register(InternalRow.class);
      kryo.register(InternalRow[].class);
      kryo.register(byte[][].class);
      kryo.register(HashMap.class);
      kryo.register(ArrayList.class);

      // TODO find more efficient ways
      kryo.register(SerializablePipelineOptions.class, new JavaSerializer());

      // side input values (spark runner specific)
      kryo.register(SideInputValues.ByWindow.class);
      kryo.register(SideInputValues.Global.class);

      // standard coders of org.apache.beam.sdk.coders
      kryo.register(AvroCoder.class);
      kryo.register(AvroGenericCoder.class);
      kryo.register(BigDecimalCoder.class);
      kryo.register(BigEndianIntegerCoder.class);
      kryo.register(BigEndianLongCoder.class);
      kryo.register(BigEndianShortCoder.class);
      kryo.register(BigIntegerCoder.class);
      kryo.register(BitSetCoder.class);
      kryo.register(BooleanCoder.class);
      kryo.register(ByteArrayCoder.class);
      kryo.register(ByteCoder.class);
      kryo.register(CollectionCoder.class);
      kryo.register(DelegateCoder.class);
      kryo.register(DequeCoder.class);
      kryo.register(DoubleCoder.class);
      kryo.register(DurationCoder.class);
      kryo.register(FloatCoder.class);
      kryo.register(InstantCoder.class);
      kryo.register(IterableCoder.class);
      kryo.register(KvCoder.class);
      kryo.register(LengthPrefixCoder.class);
      kryo.register(ListCoder.class);
      kryo.register(MapCoder.class);
      kryo.register(NullableCoder.class);
      kryo.register(RowCoder.class);
      kryo.register(SerializableCoder.class);
      kryo.register(SetCoder.class);
      kryo.register(ShardedKeyCoder.class);
      kryo.register(SnappyCoder.class);
      kryo.register(SortedMapCoder.class);
      kryo.register(StringDelegateCoder.class);
      kryo.register(StringUtf8Coder.class);
      kryo.register(TextualIntegerCoder.class);
      kryo.register(TimestampPrefixingWindowCoder.class);
      kryo.register(VarIntCoder.class);
      kryo.register(VarLongCoder.class);
      kryo.register(VoidCoder.class);

      // bounded windows and windowed value coders
      kryo.register(GlobalWindow.Coder.class);
      kryo.register(IntervalWindow.IntervalWindowCoder.class);
      kryo.register(WindowedValue.FullWindowedValueCoder.class);
      kryo.register(WindowedValue.ParamWindowedValueCoder.class);
      kryo.register(WindowedValue.ValueOnlyWindowedValueCoder.class);

      // various others
      kryo.register(OffsetRange.Coder.class);
      kryo.register(UnionCoder.class);
      kryo.register(PCollectionViews.ValueOrMetadataCoder.class);
      kryo.register(FileBasedSink.FileResultCoder.class);
      kryo.register(CoGbkResult.CoGbkResultCoder.class);
      kryo.register(CoGbkResultSchema.class);
      kryo.register(TupleTag.class);
      kryo.register(TupleTagList.class);
    }
  }
}
