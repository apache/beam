/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;


public class HadoopInputFormatIO {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatIO.class);

  public static <K, V> Read<K, V> read() {
    return new AutoValue_HadoopInputFormatIO_Read.Builder<K, V>().build();
  }

  @AutoValue
  public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
    
    private static final long serialVersionUID = 1L;
    
    @Nullable public abstract SimpleFunction<?, K> getSimpleFuncForKeyTranslation();
    @Nullable public abstract SimpleFunction<?, V> getSimpleFuncForValueTranslation();
    @Nullable public abstract SerializableConfiguration getConfiguration();
    @Nullable public abstract TypeDescriptor<K> getKeyClass();
    @Nullable public abstract TypeDescriptor<V> getValueClass();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConfiguration(SerializableConfiguration configuration);
      abstract Builder<K, V> setSimpleFuncForKeyTranslation(
          SimpleFunction<?, K> simpleFuncForKeyTranslation);
      abstract Builder<K, V> setSimpleFuncForValueTranslation(
          SimpleFunction<?, V> simpleFuncForValueTranslation);
      abstract Builder<K, V> setKeyClass(TypeDescriptor<K> keyClass);
      abstract Builder<K, V> setValueClass(TypeDescriptor<V> valueClass);
      abstract Read<K, V> build();
    }

    public Read<K, V> withConfiguration(Configuration configuration) {
      checkNotNull(configuration, "Configuration cannot be null.");
      TypeDescriptor<K> inputFormatKeyClass =
          (TypeDescriptor<K>) TypeDescriptor.of(configuration.getClass("key.class", Object.class));
      TypeDescriptor<V> inputFormatValueClass = (TypeDescriptor<V>) TypeDescriptor
          .of(configuration.getClass("value.class", Object.class));
      if (this.getKeyClass() != null && this.getValueClass() != null) {
        return toBuilder()
            .setConfiguration(new SerializableConfiguration(configuration)).build();
      }
      if (this.getKeyClass() == null && this.getValueClass() != null) {
        return toBuilder()
            .setConfiguration(new SerializableConfiguration(configuration))
            .setKeyClass(inputFormatKeyClass).build();
      }
      if (this.getKeyClass() != null && this.getValueClass() == null) {
        return toBuilder()
            .setConfiguration(new SerializableConfiguration(configuration))
            .setValueClass(inputFormatValueClass).build();
      }
      return toBuilder()
          .setConfiguration(new SerializableConfiguration(configuration))
          .setKeyClass(inputFormatKeyClass)
          .setValueClass(inputFormatValueClass).build();

    }

    public Read<K, V> withKeyTranslation(SimpleFunction<?, K> simpleFuncForKeyTranslation) {
      checkNotNull(simpleFuncForKeyTranslation,
          "Simple function for key translation cannot be null.");
      return toBuilder()
          .setSimpleFuncForKeyTranslation(simpleFuncForKeyTranslation)
          .setKeyClass((TypeDescriptor<K>) simpleFuncForKeyTranslation.getOutputTypeDescriptor())
          .build();
    }

    public Read<K, V> withValueTranslation(SimpleFunction<?, V> simpleFuncForValueTranslation) {
      checkNotNull(simpleFuncForValueTranslation,
          "Simple function for value translation cannot be null.");
      return toBuilder()
          .setSimpleFuncForValueTranslation(simpleFuncForValueTranslation)
          .setValueClass(
              (TypeDescriptor<V>) simpleFuncForValueTranslation.getOutputTypeDescriptor())
          .build();
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin input) {
      HadoopInputFormatBoundedSource<K, V> source = new HadoopInputFormatBoundedSource<K, V>(
          this.getConfiguration(), this.getKeyCoder(), this.getValueCoder(),
          this.getSimpleFuncForKeyTranslation(), this.getSimpleFuncForValueTranslation(), null);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    public void validate(PBegin input) {
      checkNotNull(this.getConfiguration(),
          "Need to set the configuration of a HadoopInputFormatIO "
              + "Read using method Read.withConfiguration().");
      String inputFormatClassProperty =
          this.getConfiguration().getConfiguration().get("mapreduce.job.inputformat.class");
      if (inputFormatClassProperty == null) {
        throw new IllegalArgumentException("Hadoop InputFormat class property "
            + "\"mapreduce.job.inputformat.class\" is not set in configuration.");
      }
      String keyClassProperty = this.getConfiguration().getConfiguration().get("key.class");
      if (keyClassProperty == null) {
        throw new IllegalArgumentException("Configuration property \"key.class\" is not set.");
      }
      String valueClassProperty = this.getConfiguration().getConfiguration().get("value.class");
      if (valueClassProperty == null) {
        throw new IllegalArgumentException("Configuration property \"value.class\" is not set.");
      }
      if (this.getSimpleFuncForKeyTranslation() != null) {
        TypeDescriptor<K> inputFormatKeyClass = (TypeDescriptor<K>) TypeDescriptor
            .of(this.getConfiguration().getConfiguration().getClass("key.class", Object.class));
        if (!this.getSimpleFuncForKeyTranslation().getInputTypeDescriptor()
            .equals(inputFormatKeyClass)) {
          throw new IllegalArgumentException(
              "Key translation's input type is not same as hadoop input format : "
                  + inputFormatClassProperty + " key class : " + keyClassProperty);
        }
      }
      if (this.getSimpleFuncForValueTranslation() != null) {
        TypeDescriptor<V> inputFormatValueClass = (TypeDescriptor<V>) TypeDescriptor
            .of(this.getConfiguration().getConfiguration().getClass("value.class", Object.class));
        if (!this.getSimpleFuncForValueTranslation().getInputTypeDescriptor()
            .equals(inputFormatValueClass)) {
          throw new IllegalArgumentException(
              "Value translation's input type is not same as hadoop input format : "
                  + inputFormatClassProperty + " value class : " + valueClassProperty);
        }
      }
      getKeyAndValueCoder(input);
    }
    
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;

    protected void getKeyAndValueCoder(PBegin input) {
      keyCoder = getDefaultCoder(this.getKeyClass(), input.getPipeline().getCoderRegistry());
      valueCoder = getDefaultCoder(this.getValueClass(), input.getPipeline().getCoderRegistry());
    }

    public <T> Coder<T> getDefaultCoder(TypeDescriptor<?> typeDesc, CoderRegistry coderRegistry) {
      Class classType = typeDesc.getRawType();
      if (Writable.class.isAssignableFrom(classType)) {
        return (Coder<T>) WritableCoder.of(classType);
      } else {
        try {
          return (Coder<T>) coderRegistry.getCoder(typeDesc);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Cannot find coder for " + typeDesc + e.getMessage());
        }
      }
    }

    public Coder<K> getKeyCoder() {
      return keyCoder;
    }

    public Coder<V> getValueCoder() {
      return valueCoder;
    }
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (this.getConfiguration().getConfiguration() != null) {
        Iterator<Entry<String, String>> propertyElement =
            this.getConfiguration().getConfiguration().iterator();
        while (propertyElement.hasNext()) {
          Entry<String, String> element = propertyElement.next();
          builder
          .add(DisplayData.item(element.getKey(), element.getValue()).withLabel(element.getKey()));
        }
      }
      builder
      .addIfNotNull(DisplayData.item("KeyClass", getKeyClass().getRawType())
          .withLabel("Output key class"))
      .addIfNotNull(DisplayData.item("ValueClass", getValueClass().getRawType())
          .withLabel("Output value class"));

    }
  }

  public static class HadoopInputFormatBoundedSource<K, V> extends BoundedSource<KV<K, V>>
  implements Serializable {
    private static final long serialVersionUID = 0L;
    protected final SerializableConfiguration conf;
    protected final SerializableSplit serializableSplit;
    protected final Coder<K> keyCoder;
    protected final Coder<V> valueCoder;
    protected final SimpleFunction<?, K> simpleFuncForKeyTranslation;
    protected final SimpleFunction<?, V> simpleFuncForValueTranslation;

    public HadoopInputFormatBoundedSource(
        SerializableConfiguration conf, 
        Coder<K> keyCoder,
        Coder<V> valueCoder) {
      this(conf, keyCoder, valueCoder, null, null, null);
    }

    public HadoopInputFormatBoundedSource(
        SerializableConfiguration conf, 
        Coder<K> keyCoder,
        Coder<V> valueCoder, 
        SimpleFunction<?, K> keyTranslation,
        SimpleFunction<?, V> valueTranslation, 
        SerializableSplit serializableSplit) {
      this.conf = conf;
      this.serializableSplit = serializableSplit;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.simpleFuncForKeyTranslation = keyTranslation;
      this.simpleFuncForValueTranslation = valueTranslation;
    }

    public SerializableConfiguration getConfiguration() {
      return conf;
    }

    @Override
    public void validate() {
      checkNotNull(conf, "Need to set the configuration of a HadoopInputFormatSource");
      checkNotNull(keyCoder, "KeyCoder should not be null in HadoopInputFormatSource");
      checkNotNull(valueCoder, "ValueCoder should not be null in HadoopInputFormatSource");
    }
    

    private boolean isSourceSplit = false;
    
    @Override
    public List<BoundedSource<KV<K, V>>> splitIntoBundles(
        long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      if (serializableSplit == null) {
        if (serInputSplitList == null) {
          computeSplits();
        }
        LOG.info("Generated {} splits each of size {} ", serInputSplitList.size(),
            serInputSplitList.get(0).getSplit().getLength());
        isSourceSplit = true;
        return Lists.transform(serInputSplitList,
            new Function<SerializableSplit, BoundedSource<KV<K, V>>>() {
          @Override
          public BoundedSource<KV<K, V>> apply(SerializableSplit serializableInputSplit) {
            HadoopInputFormatBoundedSource<K, V> hifBoundedSource =
                new HadoopInputFormatBoundedSource<K, V>(conf, keyCoder, valueCoder,
                    simpleFuncForKeyTranslation, simpleFuncForValueTranslation,
                    serializableInputSplit);
            return hifBoundedSource;
          }
        });
      } else {
        isSourceSplit = true;
        LOG.info("Not splitting source {} because source is already split.", this);
        return ImmutableList.of((BoundedSource<KV<K, V>>) this);
      }
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
      if (serInputSplitList == null) {
        computeSplits();
      }
      return boundedSourceEstimatedSize;
    }
    
    private long boundedSourceEstimatedSize = 0;
    private List<SerializableSplit> serInputSplitList;
    
    private void computeSplits() 
        throws  IOException, 
        IllegalAccessException, 
        InstantiationException,
        InterruptedException, 
        ClassNotFoundException {
      Job job = Job.getInstance(conf.getConfiguration());
      List<InputSplit> splits = job.getInputFormatClass().newInstance().getSplits(job);
      if (splits == null) {
        throw new IOException("Cannot split the source as getSplits() is returning null value.");
      }
      if (splits.isEmpty()) {
        throw new IOException("Cannot split the source as getSplits() is returning empty list.");
      }
      boundedSourceEstimatedSize = 0;
      serInputSplitList = new ArrayList<SerializableSplit>();
      for (InputSplit inputSplit : splits) {
        boundedSourceEstimatedSize = boundedSourceEstimatedSize + inputSplit.getLength();
        serInputSplitList.add(new SerializableSplit(inputSplit));
      }
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions po) throws Exception {
      return false;
    }

    @Override
    public Coder<KV<K, V>> getDefaultOutputCoder() {
      return KvCoder.of(keyCoder, valueCoder);
    }

    @Override
    public BoundedReader<KV<K, V>> createReader(PipelineOptions options) throws IOException {
      this.validate();
      if (serializableSplit == null) {
        if (!isSourceSplit) {
          throw new IOException("Cannot create reader as source is not split yet.");
        } else {
          throw new IOException("Cannot read data as split is null.");
        }
      } else {
        return new HadoopInputFormatReader<Object>(this, simpleFuncForKeyTranslation,
            simpleFuncForValueTranslation, serializableSplit.getSplit());
      }
    }

    class HadoopInputFormatReader<T extends Object> extends BoundedSource.BoundedReader<KV<K, V>> {


      private final HadoopInputFormatBoundedSource<K, V> source;
      private final InputSplit split;
      private final SimpleFunction<T, K> simpleFuncForKeyTranslation;
      private final SimpleFunction<T, V> simpleFuncForValueTranslation;

      public HadoopInputFormatReader(
          HadoopInputFormatBoundedSource<K, V> source,
          SimpleFunction keyTranslation, 
          SimpleFunction valueTranslation, 
          InputSplit split){
        this.source = source;
        this.split = split;
        this.simpleFuncForKeyTranslation = keyTranslation;
        this.simpleFuncForValueTranslation = valueTranslation;
      }

      @Override
      public HadoopInputFormatBoundedSource<K, V> getCurrentSource() {
        return source;
      }

      private RecordReader<T, T> currentReader;
      private KV<K, V> currentPair;
      private volatile boolean done = false;
      private long recordsReturned = 0L;
      
      @Override
      public boolean start() throws IOException {
        try {
          TaskAttemptContextImpl attemptContext = new TaskAttemptContextImpl(source.getConfiguration().getConfiguration(),
              new TaskAttemptID());
          Job job = Job.getInstance(source.getConfiguration().getConfiguration());
          InputFormat<?, ?> inputFormatObj = (InputFormat<?, ?>) job.getInputFormatClass().newInstance();
          RecordReader<?,?> reader = inputFormatObj.createRecordReader(split, attemptContext);
          if (currentReader != null) {
            currentReader.close();
          }
          currentReader = (RecordReader<T, T>) reader;
          currentReader.initialize(split, attemptContext);
          if (currentReader.nextKeyValue()) {
            currentPair = nextPair();
            return true;
          }
        } catch (Exception e) {
          throw new IOException(e);
        } 
        currentReader = null;
        currentPair = null;
        done = true;
        return false;
      }

      @Override
      public boolean advance() throws IOException {
        try {
          if (currentReader != null && currentReader.nextKeyValue()) {
            currentPair = nextPair();
            return true;
          }
          currentPair = null;
          done = true;
          return false;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }


      public KV<K, V> nextPair() throws Exception {
        K key;
        V value;
        if (null != simpleFuncForKeyTranslation) {
          key = simpleFuncForKeyTranslation.apply(currentReader.getCurrentKey());
        } else {
          key = (K) currentReader.getCurrentKey();
        }

        if (null != simpleFuncForValueTranslation) {
          value = simpleFuncForValueTranslation.apply(currentReader.getCurrentValue());
        } else {
          value = (V) currentReader.getCurrentValue();
        }
        recordsReturned++;
        return cloneKeyValue(key, value);
      }

      // Cloning/Serialization is required to take care of Hadoop's mutable objects if returned by
      // RecordReader as Beam needs immutable objects
      private KV<K, V> cloneKeyValue(K key, V value) throws IOException, InterruptedException {
        if (!isKnownImmutable(key)) {
          try {
            key = (K) CoderUtils.clone((Coder<K>) keyCoder, (K) key);
          } catch (ClassCastException ex) {
            throw new ClassCastException(
                "KeyClass set in configuration " + conf.getConfiguration().get("key.class")
                + " is not compatible with keyClass of record reader "
                + currentReader.getCurrentKey().getClass().toString());
          }
        }
        if (!isKnownImmutable(value)) {
          try {
            value = (V) CoderUtils.clone((Coder<V>) valueCoder, (V) value);
          } catch (ClassCastException ex) {
            throw new ClassCastException(
                "ValueClass set in configuration " + conf.getConfiguration().get("value.class")
                + " is not compatible with valueClass of record reader "
                + currentReader.getCurrentValue().getClass().toString());
          }
        }
        return KV.of(key, value);
      }


      private boolean isKnownImmutable(Object o) {
        Set<Class<?>> immutableTypes = new HashSet<Class<?>>(
            Arrays.asList(String.class, 
                Byte.class, 
                Short.class, 
                Integer.class, 
                Long.class,
                Float.class, 
                Double.class, 
                Boolean.class, 
                BigInteger.class, 
                BigDecimal.class));
        return immutableTypes.contains(o.getClass());
      }

      @Override
      public KV<K, V> getCurrent() throws NoSuchElementException {
        if (currentPair == null) {
          throw new NoSuchElementException();
        }
        return currentPair;
      }

      @Override
      public void close() throws IOException {
        LOG.info("Closing reader after reading {} records.", recordsReturned);
        if (currentReader != null) {
          currentReader.close();
          currentReader = null;
        }
        currentPair = null;
      }

      @Override
      public Double getFractionConsumed() {
        if (currentReader == null || recordsReturned == 0) {
          return 0.0;
        }
        if (done) {
          return 1.0;
        }
        return getProgress();
      }
      // Get RecordReader Progress
      private Double getProgress() {
        try {
          return (double) currentReader.getProgress();
        } catch (IOException | InterruptedException e) {
          LOG.error(
              "Cannot compute fractions consumed as getProgress() of RecordReader throwing exception."
                  + e.getMessage());
          return null;
        }
      }

      @Override
      public final long getSplitPointsRemaining() {
        if (done) {
          return 0;
        }
        // This source does not currently support dynamic work
        // rebalancing, so remaining
        // parallelism is always 1.
        return 1;
      }
    }

    /**
     * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be serialized
     * using Java's standard serialization mechanisms. Note that the InputSplit has to be Writable
     * (which most are).
     */
    public class SerializableSplit implements Externalizable {

      private static final long serialVersionUID = 0L;

      private InputSplit split;

      public SerializableSplit() {}

      public SerializableSplit(InputSplit split) {
        checkArgument(split instanceof Writable, "Split is not writable: %s", split);
        this.split = split;
      }

      public InputSplit getSplit() {
        return split;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(split.getClass().getCanonicalName());
        ((Writable) split).write(out);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        String className = in.readUTF();
        try {
          split = (InputSplit) Class.forName(className).newInstance();
          ((Writable) split).readFields(in);
        } catch (InstantiationException | IllegalAccessException e) {
          throw new IOException(e);
        }
      }
    }
  }

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.conf.Configuration} is to be serialized
   * using Java's standard serialization mechanisms. Note that the
   * org.apache.hadoop.conf.Configuration has to be Writable (which most are).
   */
  public static class SerializableConfiguration implements Externalizable {
    private static final long serialVersionUID = 0L;

    private Configuration conf;

    public SerializableConfiguration() {}

    public SerializableConfiguration(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(conf.getClass().getCanonicalName());
      ((Writable) conf).write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String className = in.readUTF();
      try {
        conf = (Configuration) Class.forName(className).newInstance();
        conf.readFields(in);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException(e);
      }
    }
  }

}
