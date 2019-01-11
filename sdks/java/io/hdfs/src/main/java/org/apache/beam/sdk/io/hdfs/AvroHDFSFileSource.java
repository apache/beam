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
package org.apache.beam.sdk.io.hdfs;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A {@code BoundedSource} for reading Avro files resident in a Hadoop filesystem.
 *
 * @param <T> The type of the Avro records to be read from the source.
 */
public class AvroHDFSFileSource<T> extends HDFSFileSource<AvroKey<T>, NullWritable> {
  private static final long serialVersionUID = 0L;

  protected final AvroCoder<T> avroCoder;
  private final String schemaStr;

  public AvroHDFSFileSource(String filepattern, AvroCoder<T> avroCoder) {
    this(filepattern, avroCoder, null);
  }

  public AvroHDFSFileSource(String filepattern,
                            AvroCoder<T> avroCoder,
                            HDFSFileSource.SerializableSplit serializableSplit) {
    super(filepattern,
        ClassUtil.<AvroKeyInputFormat<T>>castClass(AvroKeyInputFormat.class),
        ClassUtil.<AvroKey<T>>castClass(AvroKey.class),
        NullWritable.class, serializableSplit);
    this.avroCoder = avroCoder;
    this.schemaStr = avroCoder.getSchema().toString();
  }

  @Override
  public Coder<KV<AvroKey<T>, NullWritable>> getDefaultOutputCoder() {
    AvroWrapperCoder<AvroKey<T>, T> keyCoder = AvroWrapperCoder.of(this.getKeyClass(), avroCoder);
    return KvCoder.of(keyCoder, WritableCoder.of(NullWritable.class));
  }

  @Override
  public List<? extends AvroHDFSFileSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, AvroHDFSFileSource<T>>() {
            @Override
            public AvroHDFSFileSource<T> apply(@Nullable InputSplit inputSplit) {
              return new AvroHDFSFileSource<>(
                  filepattern, avroCoder, new SerializableSplit(inputSplit));
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public BoundedReader<KV<AvroKey<T>, NullWritable>> createReader(PipelineOptions options)
      throws IOException {
    this.validate();

    Schema schema = new Schema.Parser().parse(schemaStr);
    if (serializableSplit == null) {
      return new AvroHDFSFileReader<>(this, filepattern, formatClass, schema);
    } else {
      return new AvroHDFSFileReader<>(this, filepattern, formatClass, schema,
          serializableSplit.getSplit());
    }
  }

  static class AvroHDFSFileReader<T> extends HDFSFileReader<AvroKey<T>, NullWritable> {
    public AvroHDFSFileReader(BoundedSource<KV<AvroKey<T>, NullWritable>> source,
                              String filepattern,
                              Class<? extends FileInputFormat<?, ?>> formatClass,
                              Schema schema) throws IOException {
      this(source, filepattern, formatClass, schema, null);
    }

    public AvroHDFSFileReader(BoundedSource<KV<AvroKey<T>, NullWritable>> source,
                              String filepattern,
                              Class<? extends FileInputFormat<?, ?>> formatClass,
                              Schema schema, InputSplit split) throws IOException {
      super(source, filepattern, formatClass, split);
      AvroJob.setInputKeySchema(job, schema);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected KV<AvroKey<T>, NullWritable> nextPair() throws IOException, InterruptedException {
      AvroKey<T> key = currentReader.getCurrentKey();
      NullWritable value = currentReader.getCurrentValue();

      // clone the record to work around identical element issue due to object reuse
      Coder<T> avroCoder = ((AvroHDFSFileSource<T>) this.getCurrentSource()).avroCoder;
      key = new AvroKey<>(CoderUtils.clone(avroCoder, key.datum()));

      return KV.of(key, value);
    }

  }

  static class ClassUtil {
    @SuppressWarnings("unchecked")
    static <T> Class<T> castClass(Class<?> aClass) {
      return (Class<T>) aClass;
    }
  }

}
