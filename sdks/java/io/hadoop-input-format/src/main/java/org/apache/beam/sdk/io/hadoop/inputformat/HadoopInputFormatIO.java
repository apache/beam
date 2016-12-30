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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.utils.HadoopInputFormatUtils;
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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class HadoopInputFormatIO {

	private static final Logger logger = LoggerFactory.getLogger(HadoopInputFormatIO.class);

	public static <K,V> Read<K,V> read() {
		return new AutoValue_HadoopInputFormatIO_Read.Builder<K,V>().build();
	}

	@AutoValue
	public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

		private static final long serialVersionUID = 1L;
		private TypeDescriptor<K> keyClass;
		private TypeDescriptor<V> valueClass;
		private Coder<K> keyCoder;
		private Coder<V> valueCoder;

		abstract Builder<K, V> toBuilder();
		@Nullable public abstract  SimpleFunction< ?, K> getSimpleFuncForKeyTranslation();
		@Nullable public abstract  SimpleFunction< ?, V> getSimpleFuncForValueTranslation();
		@Nullable public abstract  SerializableConfiguration getConfiguration();

		public void setKeyClass(TypeDescriptor<K> keyClass) {
			this.keyClass = keyClass;
		}

		public void setValueClass(TypeDescriptor<V> valueClass) {
			this.valueClass = valueClass;
		}

		public Coder<K> getKeyCoder() {
			return keyCoder;
		}

		public Coder<V> getValueCoder() {
			return valueCoder;
		}

		public Read<K, V> withConfiguration(Configuration configuration) {
			checkNotNull(configuration,  "Configuration cannot be null.");
			return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
		}

		public Read<K, V> withKeyTranslation(SimpleFunction<?, K> simpleFuncForKeyTranslation) {
			checkNotNull(simpleFuncForKeyTranslation, "Simple function for key translation cannot be null.");
			return toBuilder().setSimpleFuncForKeyTranslation(simpleFuncForKeyTranslation).build();
		}

		public Read<K, V> withValueTranslation(SimpleFunction< ?, V> simpleFuncForValueTranslation) {
			checkNotNull(simpleFuncForValueTranslation, "Simple function for value translation cannot be null.");
			return toBuilder().setSimpleFuncForValueTranslation(simpleFuncForValueTranslation).build();
		}

		@AutoValue.Builder
		abstract static class Builder<K, V> {
			abstract Builder<K,V> setConfiguration(SerializableConfiguration configuration);
			abstract Builder<K,V> setSimpleFuncForKeyTranslation(SimpleFunction< ?,K> simpleFuncForKeyTranslation);
			abstract Builder<K,V> setSimpleFuncForValueTranslation(SimpleFunction<?,V> simpleFuncForValueTranslation);
			abstract Read<K,V> build();
		}

		public void validate(PBegin input) {
			checkNotNull(this.getConfiguration(), "Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().");
			String inputFormatClassProperty = this.getConfiguration().getConfiguration().get("mapreduce.job.inputformat.class") ;
			if (inputFormatClassProperty == null) {
				throw new IllegalArgumentException("Hadoop InputFormat class property \"mapreduce.job.inputformat.class\" is not set in configuration.");

			}
			String keyClassProperty = this.getConfiguration().getConfiguration().get("key.class");
			if (keyClassProperty == null) {
				throw new IllegalArgumentException("Configuration property \"key.class\" is not set.");

			}
			String valueClassProperty = this.getConfiguration().getConfiguration().get("value.class");
			if (valueClassProperty == null) {
				throw new IllegalArgumentException("Configuration property \"value.class\" is not set.");

			}
			TypeDescriptor<K> inputFormatKeyClass = (TypeDescriptor<K>) TypeDescriptor.of(this.getConfiguration().getConfiguration().getClass("key.class", Object.class));
			if (this.getSimpleFuncForKeyTranslation() != null) {
				if (!this.getSimpleFuncForKeyTranslation().getInputTypeDescriptor().equals(inputFormatKeyClass)) {
					throw new IllegalArgumentException(
							"Key translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" key class : "+ keyClassProperty);

				}
				this.setKeyClass((TypeDescriptor<K>) this.getSimpleFuncForKeyTranslation().getOutputTypeDescriptor());
			} else {
				this.setKeyClass(inputFormatKeyClass);
			}

			TypeDescriptor<V> inputFormatValueClass = (TypeDescriptor<V>) TypeDescriptor.of(this.getConfiguration().getConfiguration().getClass("value.class", Object.class));
			if (this.getSimpleFuncForValueTranslation() != null) {
				if (!this.getSimpleFuncForValueTranslation().getInputTypeDescriptor().equals(inputFormatValueClass)) {
					throw new IllegalArgumentException(
							"Value translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" value class : "+ valueClassProperty);
				}
				this.setValueClass((TypeDescriptor<V>) this.getSimpleFuncForValueTranslation().getOutputTypeDescriptor());
			} else {
				this.setValueClass(inputFormatValueClass);
			}
			checkNotNull(this.getKeyClass(), "The key class of HadoopInputFormatIO Read can't be null.");
			checkNotNull(this.getValueClass(), "The value class of HadoopInputFormatIO Read can't be null.");
			getKeyAndValueCoder(input);
			checkNotNull(this.getKeyCoder(), "The key coder class of HadoopInputFormatIO Read can't be null.");
			checkNotNull(this.getValueCoder(), "The value coder class of HadoopInputFormatIO Read can't be null.");

		}

		public TypeDescriptor<K> getKeyClass() {
			return keyClass;
		}

		public TypeDescriptor<V> getValueClass() {
			return valueClass;
		}

		protected void getKeyAndValueCoder(PBegin input) {
			keyCoder = getDefaultCoder(this.getKeyClass(), input.getPipeline());
			valueCoder = getDefaultCoder(this.getValueClass(), input.getPipeline());
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			if (this.getConfiguration().getConfiguration() != null) {
				Iterator<Entry<String, String>> propertyElement = this.getConfiguration().getConfiguration().iterator();
				while (propertyElement.hasNext()) {
					Entry<String, String> element = propertyElement.next();
					builder.add(DisplayData.item(element.getKey(), element.getValue()).withLabel(element.getKey()));
				}
			}
			builder.addIfNotNull(DisplayData.item("KeyClass", getKeyClass().getRawType()).withLabel("Output key class"))
					.addIfNotNull(DisplayData.item("ValueClass", getValueClass().getRawType()).withLabel("Output value class"));

		}

		@Override
		public PCollection<KV<K, V>> expand(PBegin input) {
			HadoopInputFormatBoundedSource<K, V> source = new HadoopInputFormatBoundedSource<K, V>(this.getConfiguration(),
					this.getKeyCoder(), this.getValueCoder(), this.getSimpleFuncForKeyTranslation(),
					this.getSimpleFuncForValueTranslation(), null);
			return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
		}

		@SuppressWarnings("unchecked")
		public <T> Coder<T> getDefaultCoder(TypeDescriptor<?> typeDesc, Pipeline pipeline) {
			Class classType = typeDesc.getRawType();
			if (Writable.class.isAssignableFrom(classType)) {
				return (Coder<T>) WritableCoder.of(classType);
			} else {
				CoderRegistry coderRegistry = pipeline.getCoderRegistry();
				try {
					return (Coder<T>) coderRegistry.getCoder(typeDesc);
				} catch (CannotProvideCoderException e) {
					throw new IllegalStateException("Cannot find coder for " + typeDesc);
				}
			}
		}

	}

	public static class HadoopInputFormatBoundedSource<K, V> extends BoundedSource<KV<K, V>> implements Serializable {

		private static final long serialVersionUID = 0L;
		protected final SerializableConfiguration conf;
		protected final SerializableSplit serializableSplit;
		protected final Coder<K> keyCoder;
		protected final Coder<V> valueCoder;
		protected final SimpleFunction<?,K> simpleFuncForKeyTranslation;
		protected final SimpleFunction< ? ,V> simpleFuncForValueTranslation;

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder, Coder<V> valueCoder) {
			this(conf, keyCoder, valueCoder, null, null, null);
		}

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder, Coder<V> valueCoder,
				SimpleFunction<?,K> keyTranslation, SimpleFunction< ? ,V> valueTranslation, SerializableSplit serializableSplit) {
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
		boolean isSplitted=false;
		@Override
		public List<BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,PipelineOptions options) throws Exception {
			// add comments
			if (serializableSplit == null) {
				if (serInputSplitList == null) {
					computeSplits();
				}
				return Lists.transform(serInputSplitList, new Function<SerializableSplit, BoundedSource<KV<K, V>>>() {
					@Override
					public BoundedSource<KV<K, V>> apply(SerializableSplit serializableInputSplit) {
						HadoopInputFormatBoundedSource<K, V> hifBoundedSource = new HadoopInputFormatBoundedSource<K, V>(
								conf, keyCoder, valueCoder, simpleFuncForKeyTranslation, simpleFuncForValueTranslation,
								serializableInputSplit);
						isSplitted=true;
						return hifBoundedSource;
					}
				});
			} else {
				isSplitted=true;
				return ImmutableList.of((BoundedSource<KV<K, V>>) this);
			}

		}

		@VisibleForTesting
		private void computeSplits() throws IOException, IllegalAccessException, InstantiationException,
		InterruptedException, ClassNotFoundException {

			Job job = Job.getInstance(conf.getConfiguration());
			List<InputSplit> splits = job.getInputFormatClass().newInstance().getSplits(job);
			if(splits == null)
				throw new IOException("Cannot split the source as getSplits() is returning null value.");
			if(splits.isEmpty())
				throw new IOException("Cannot split the source as getSplits() is returning empty list.");
			boundedSourceEstimatedSize=0;
			serInputSplitList = new ArrayList<SerializableSplit>();
			for (InputSplit inputSplit :splits) {
				boundedSourceEstimatedSize = boundedSourceEstimatedSize + inputSplit.getLength();
				serInputSplitList.add(new SerializableSplit(inputSplit));
			}

		}

		List<SerializableSplit> serInputSplitList;
		long boundedSourceEstimatedSize = 0;

		@Override
		public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
			if (serInputSplitList == null) {
				computeSplits();
			}
			return boundedSourceEstimatedSize;
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
				if(!isSplitted)
					throw new IOException("Cannot create reader as source is not split yet.");
				else
					throw new IOException("Cannot read data as split is null.");
			} else {
				try {
					return new HadoopInputFormatReader<Object>(this, simpleFuncForKeyTranslation,simpleFuncForValueTranslation, serializableSplit.getSplit());
				} catch (InstantiationException e) {
					throw new IOException("InstantiationException : Unable to create reader.");
				} catch (IllegalAccessException e) {
					throw new IOException("Unable to create reader. Couldnot create job object.");
				} catch (ClassNotFoundException e) {
					throw new IOException("Unable to create reader. Couldnot create job object Class not found "+e.getClass());
				}
			}

		}

		class HadoopInputFormatReader<T extends Object> extends BoundedSource.BoundedReader<KV<K, V>> {

			private InputFormat inputFormatObj;
			private TaskAttemptContextImpl attemptContext;
			private List<InputSplit> splits;
			private ListIterator<InputSplit> splitsIterator;
			private  RecordReader<T,T>  currentReader;
			private KV<K, V> currentPair;
			private volatile boolean done = false;
			private final HadoopInputFormatBoundedSource<K, V> source;
			private final SimpleFunction< T, K> simpleFuncForKeyTranslation;
			private final SimpleFunction< T, V> simpleFuncForValueTranslation;


			public HadoopInputFormatReader(HadoopInputFormatBoundedSource<K,V> source, SimpleFunction keyTranslation,
					SimpleFunction valueTranslation, InputSplit split) throws IOException , InstantiationException , IllegalAccessException , ClassNotFoundException  {
				this.source = source;
				Job job =  Job.getInstance(source.getConfiguration().getConfiguration());
				inputFormatObj = (InputFormat) job.getInputFormatClass().newInstance();
				if (split != null) {
					this.splits = ImmutableList.of(split);
					this.splitsIterator = splits.listIterator();
				}
				this.simpleFuncForKeyTranslation = keyTranslation;
				this.simpleFuncForValueTranslation = valueTranslation;
			}

			@Override
			public HadoopInputFormatBoundedSource<K,V> getCurrentSource() {
				return source;
			}

			@Override
			public boolean start() throws IOException {
				attemptContext = new TaskAttemptContextImpl(source.getConfiguration().getConfiguration(),
						new TaskAttemptID());
				InputSplit nextSplit = splitsIterator.next();
				try {
					@SuppressWarnings("unchecked")
					RecordReader<T, T> reader = (RecordReader<T, T>) inputFormatObj.createRecordReader(nextSplit,attemptContext);
					if (currentReader != null) {
						currentReader.close();
					}
					currentReader = reader;
					currentReader.initialize(nextSplit, attemptContext);
					if (currentReader.nextKeyValue()) {
						currentPair = nextPair();
						return true;
					}
					currentReader.close();
					currentReader = null;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException(e);
				}
			}


			public KV<K, V> nextPair() throws IOException, InterruptedException {
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
				return cloneKeyValue(key,value);
			}

			private KV<K, V> cloneKeyValue(K key,V value) throws IOException, InterruptedException {

				if (!HadoopInputFormatUtils.isImmutable(key)) {
					try {
						key = (K) CoderUtils.clone((Coder<K>)keyCoder,(K) key);
					} 
					catch (ClassCastException ex) {
						throw new ClassCastException("KeyClass set in configuration "+conf.getConfiguration().get("key.class")+" is not compatible with keyClass of record reader "+currentReader.getCurrentKey().getClass().toString());
					}
				}
				if (!HadoopInputFormatUtils.isImmutable(value)) {
					try{
						value = (V) CoderUtils.clone((Coder<V>)valueCoder, (V) value);
					} 
					catch (ClassCastException ex) {
						throw new ClassCastException("ValueClass set in configuration "+conf.getConfiguration().get("value.class")+" is not compatible with valueClass of record reader "+currentReader.getCurrentValue().getClass().toString());
					}
				}
				return KV.of(key,value);
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
				if (currentReader != null) {
					currentReader.close();
					currentReader = null;
				}
				currentPair = null;
			}

			@Override
			public Double getFractionConsumed() {
				if (currentReader == null) {
					return 0.0;
				}
				if (splits.isEmpty()) {
					return 1.0;
				}
				int index = splitsIterator.previousIndex();
				int numReaders = splits.size();
				if (index == numReaders) {
					return 1.0;
				}
				double before = 1.0 * index / numReaders;
				double after = 1.0 * (index + 1) / numReaders;
				Double fractionOfCurrentReader = getProgress();
				if (fractionOfCurrentReader == null) {
					return before;
				}
				return before + fractionOfCurrentReader * (after - before);
			}

			private Double getProgress() {
				try {
					return (double) currentReader.getProgress();
				} catch (IOException | InterruptedException e) {
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
		 * A wrapper to allow Hadoop
		 * {@link org.apache.hadoop.mapreduce.InputSplit}s to be serialized
		 * using Java's standard serialization mechanisms. Note that the
		 * InputSplit has to be Writable (which most are).
		 */
		public class SerializableSplit implements Externalizable {

			private static final long serialVersionUID = 0L;

			private InputSplit split;

			public SerializableSplit() {
			}

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

	public static class SerializableConfiguration implements Externalizable {
		private static final long serialVersionUID = 0L;

		private Configuration conf;

		public SerializableConfiguration() {
		}

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
