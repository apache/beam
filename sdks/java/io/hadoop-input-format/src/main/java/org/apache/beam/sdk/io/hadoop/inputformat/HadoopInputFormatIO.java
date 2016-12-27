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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 * @author sheetal_tarodekar
 */
public class HadoopInputFormatIO {

	private static final Logger logger = LoggerFactory.getLogger(HadoopInputFormatIO.class);

	public static Read read() {
		return new Read.ReadBuilder().build();
	}

	public static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

		private static final long serialVersionUID = 1L;
		private final SimpleFunction<?, ?> simpleFuncForKeyTranslation;
		private final SimpleFunction<?, ?> simpleFuncForValueTranslation;
		private final SerializableConfiguration configuration;
		private Class<K> keyClass;
		private Class<V> valueClass;
		private Coder<K> keyCoder;
		private Coder<V> valueCoder;

		public SimpleFunction<?, ?> getSimpleFuncForKeyTranslation() {
			return simpleFuncForKeyTranslation;
		}

		public SimpleFunction<?, ?> getSimpleFuncForValueTranslation() {
			return simpleFuncForValueTranslation;
		}

		public void setKeyClass(Class<K> keyClass) {
			this.keyClass = keyClass;
		}

		public void setValueClass(Class<V> valueClass) {
			this.valueClass = valueClass;
		}

		public Class<K> getKeyClass() {
			return keyClass;
		}

		public Class<V> getValueClass() {
			return valueClass;
		}

		public Coder<K> getKeyCoder() {
			return keyCoder;
		}

		public Coder<V> getValueCoder() {
			return valueCoder;
		}

		public SerializableConfiguration getConfiguration() {
			return configuration;
		}

		public Read<K, V> withConfiguration(Configuration configuration) {
			checkNotNull(configuration,  "Configuration cannot be null.");
			return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
		}

		public Read<K, V> withKeyTranslation(SimpleFunction<?, ?> simpleFuncForKeyTranslation) {
			checkNotNull(simpleFuncForKeyTranslation, "Simple function for key translation cannot be null.");
			return toBuilder().setKeyTranslation(simpleFuncForKeyTranslation).build();
		}

		public Read<K, V> withValueTranslation(SimpleFunction<?, ?> simpleFuncForValueTranslation) {
			checkNotNull(simpleFuncForValueTranslation, "Simple function for value translation cannot be null.");
			return toBuilder().setValueTranslation(simpleFuncForValueTranslation).build();
		}

		private Read(ReadBuilder<K, V> builder) {
			this.configuration = builder.configuration;
			this.simpleFuncForKeyTranslation = builder.simpleFuncForKeyTranslation;
			this.simpleFuncForValueTranslation = builder.simpleFuncForValueTranslation;
		}

		public ReadBuilder<K, V> toBuilder() {
			return new ReadBuilder<K, V>(this);
		}

		public static class ReadBuilder<K, V> {
			private SerializableConfiguration configuration;
			private SimpleFunction<?, ?> simpleFuncForKeyTranslation;
			private SimpleFunction<?, ?> simpleFuncForValueTranslation;

			public ReadBuilder(Read<K, V> read) {
				this.configuration = read.getConfiguration();
				this.simpleFuncForKeyTranslation = read.getSimpleFuncForKeyTranslation();
				this.simpleFuncForValueTranslation = read.getSimpleFuncForValueTranslation();
			}

			public ReadBuilder() {
			}

			public ReadBuilder<K, V> setConfiguration(SerializableConfiguration configuration) {
				this.configuration = configuration;
				return this;
			}

			public ReadBuilder<K, V> setKeyTranslation(SimpleFunction<?, ?> simpleFuncForKeyTranslation) {
				this.simpleFuncForKeyTranslation = simpleFuncForKeyTranslation;
				return this;
			}

			public ReadBuilder<K, V> setValueTranslation(SimpleFunction<?, ?> simpleFuncForValueTranslation) {
				this.simpleFuncForValueTranslation = simpleFuncForValueTranslation;
				return this;
			}

			public Read<K, V> build() {
				return new Read<K, V>(this);
			}

		}

		public void validate(PBegin input) {
			checkNotNull(this.getConfiguration(), "Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().");
			String inputFormatClassProperty = configuration.getConfiguration().get("mapreduce.job.inputformat.class") ;
			if (inputFormatClassProperty == null) {
				throw new IllegalArgumentException("Hadoop InputFormat class property \"mapreduce.job.inputformat.class\" is not set in configuration.");

			}
			String keyClassProperty = configuration.getConfiguration().get("key.class");
			if (keyClassProperty == null) {
				throw new IllegalArgumentException("Configuration property \"key.class\" is not set.");

			}
			String valueClassProperty = configuration.getConfiguration().get("value.class");
			if (valueClassProperty == null) {
				throw new IllegalArgumentException("Configuration property \"value.class\" is not set.");

			}
			Class<?> inputFormatKeyClass = configuration.getConfiguration().getClass("key.class", Object.class);
			if (this.getSimpleFuncForKeyTranslation() != null) {
				if (this.getSimpleFuncForKeyTranslation().getInputTypeDescriptor()
						.getRawType() != inputFormatKeyClass) {
					throw new IllegalArgumentException(	"Key translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" key class : "+ keyClassProperty);

				}
				this.setKeyClass((Class<K>) this.getSimpleFuncForKeyTranslation().getOutputTypeDescriptor().getRawType());
			} else {
				this.setKeyClass((Class<K>) inputFormatKeyClass);
			}

			Class<?> inputFormatValueClass = configuration.getConfiguration().getClass("value.class", Object.class);
			if (this.getSimpleFuncForValueTranslation() != null) {
				if (this.getSimpleFuncForValueTranslation().getInputTypeDescriptor()
						.getRawType() != inputFormatValueClass) {
					throw new IllegalArgumentException("Value translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" value class : "+ valueClassProperty);
				}
				this.setValueClass((Class<V>) this.getSimpleFuncForValueTranslation().getOutputTypeDescriptor().getRawType());
			} else {
				this.setValueClass((Class<V>) inputFormatValueClass);
			}
			checkNotNull(this.getKeyClass(), "The key class of HadoopInputFormatIO Read can't be null.");
			checkNotNull(this.getValueClass(), "The value class of HadoopInputFormatIO Read can't be null.");
			getKeyAndValueCoder(input);
			checkNotNull(this.getKeyCoder(), "The key coder class of HadoopInputFormatIO Read can't be null.");
			checkNotNull(this.getValueCoder(), "The value coder class of HadoopInputFormatIO Read can't be null.");

		}

		protected void getKeyAndValueCoder(PBegin input) {
			keyCoder = getDefaultCoder(TypeDescriptor.of(this.getKeyClass()), input.getPipeline());
			valueCoder = getDefaultCoder(TypeDescriptor.of(this.getValueClass()), input.getPipeline());
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			if (configuration.getConfiguration() != null) {
				Iterator<Entry<String, String>> propertyElement = configuration.getConfiguration().iterator();
				while (propertyElement.hasNext()) {
					Entry<String, String> element = propertyElement.next();
					builder.add(DisplayData.item(element.getKey(), element.getValue()).withLabel(element.getKey()));
				}
			}
			builder.addIfNotNull(DisplayData.item("KeyClass", getKeyClass()).withLabel("Output key class"))
					.addIfNotNull(DisplayData.item("ValueClass", getValueClass()).withLabel("Output value class"));

		}

		@Override
		public PCollection<KV<K, V>> expand(PBegin input) {
			HadoopInputFormatBoundedSource<K, V> source = new HadoopInputFormatBoundedSource<K, V>(configuration,
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
		protected final Coder keyCoder;
		protected final Coder valueCoder;
		protected final SimpleFunction simpleFuncForKeyTranslation;
		protected final SimpleFunction simpleFuncForValueTranslation;

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder keyCoder, Coder valueCoder) {
			this(conf, keyCoder, valueCoder, null, null, null);
		}

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder keyCoder, Coder valueCoder,
				SimpleFunction keyTranslation, SimpleFunction valueTranslation, SerializableSplit serializableSplit) {
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

		@Override
		public List<? extends BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
				PipelineOptions options) throws Exception {
			// add comments
			if (serializableSplit == null) {
				return Lists.transform(inputSplitList, new Function<SerializableSplit, BoundedSource<KV<K, V>>>() {
					@Override
					public BoundedSource<KV<K, V>> apply(SerializableSplit serializableInputSplit) {
						HadoopInputFormatBoundedSource<K, V> hifBoundedSource = new HadoopInputFormatBoundedSource<K, V>(
								conf, keyCoder, valueCoder, simpleFuncForKeyTranslation, simpleFuncForValueTranslation,
								serializableInputSplit);
						return hifBoundedSource;
					}
				});
			} else {
				return ImmutableList.of(this);
			}
		}

		protected List<InputSplit> computeSplits() throws IOException, IllegalAccessException, InstantiationException,
				InterruptedException, ClassNotFoundException {
			@SuppressWarnings("deprecation")
			Job job = Job.getInstance(conf.getConfiguration());
			List<InputSplit> splits = job.getInputFormatClass().newInstance().getSplits(job);
			return splits;
		}

		List<SerializableSplit> inputSplitList;
		long boundedSourceEstimatedSize = 0;

		@Override
		public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {

			if (inputSplitList == null) {
				inputSplitList = new ArrayList<SerializableSplit>();
				for (InputSplit inputSplit : computeSplits()) {
					boundedSourceEstimatedSize = boundedSourceEstimatedSize + inputSplit.getLength();
					inputSplitList.add(new SerializableSplit(inputSplit));
				}
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
				throw new IOException("Cannot read data as split is null.");
			} else {
				return new HadoopInputFormatReader<K, V>(this, simpleFuncForKeyTranslation,
						simpleFuncForValueTranslation, serializableSplit.getSplit());
			}

		}

		class HadoopInputFormatReader<K, V> extends BoundedSource.BoundedReader<KV<K, V>> {

			private InputFormat inputFormatObj;
			private TaskAttemptContextImpl attemptContext;
			private List<InputSplit> splits;
			private ListIterator<InputSplit> splitsIterator;
			private RecordReader currentReader;
			private KV<K, V> currentPair;
			private volatile boolean done = false;
			private final HadoopInputFormatBoundedSource<K, V> source;
			private final SimpleFunction simpleFuncForKeyTranslation;
			private final SimpleFunction simpleFuncForValueTranslation;

			@SuppressWarnings("deprecation")
			public HadoopInputFormatReader(HadoopInputFormatBoundedSource source, SimpleFunction keyTranslation,
					SimpleFunction valueTranslation, InputSplit split) {
				this.source = source;
				Job job;
				try {
					job =  Job.getInstance(source.getConfiguration().getConfiguration());
					inputFormatObj = (InputFormat) job.getInputFormatClass().newInstance();
				} catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					// throw custom exception
					e.printStackTrace();
				}

				if (split != null) {
					this.splits = ImmutableList.of(split);
					this.splitsIterator = splits.listIterator();
				}
				this.simpleFuncForKeyTranslation = keyTranslation;
				this.simpleFuncForValueTranslation = valueTranslation;
			}

			@Override
			public HadoopInputFormatBoundedSource getCurrentSource() {
				return source;
			}

			@Override
			public boolean start() throws IOException {
				attemptContext = new TaskAttemptContextImpl(source.getConfiguration().getConfiguration(),
						new TaskAttemptID());
				InputSplit nextSplit = splitsIterator.next();
				try {
					@SuppressWarnings("unchecked")
					RecordReader<K, V> reader = (RecordReader<K, V>) inputFormatObj.createRecordReader(nextSplit,
							attemptContext);
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
					key = (K) simpleFuncForKeyTranslation.apply(currentReader.getCurrentKey());
				} else {
					key = (K) currentReader.getCurrentKey();
				}

				if (null != simpleFuncForValueTranslation) {
					value = (V) simpleFuncForValueTranslation.apply(currentReader.getCurrentValue());
				} else {
					value = (V) currentReader.getCurrentValue();
				}

				if (!HadoopInputFormatUtils.isImmutable(key)) {
					key = (K) CoderUtils.clone(keyCoder, key);
				}
				if (!HadoopInputFormatUtils.isImmutable(value)) {
					value = (V) CoderUtils.clone(valueCoder, value);
				}

				return KV.of(key, value);
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
