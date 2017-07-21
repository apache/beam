package org.apache.beam.runners.mapreduce;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import javax.annotation.Nullable;
import org.apache.beam.runners.mapreduce.translation.BeamInputFormat;
import org.apache.beam.runners.mapreduce.translation.BeamMapper;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MapReduceWordCount {

  public static class CreateSource<T> extends OffsetBasedSource<T> {
    private final List<byte[]> allElementsBytes;
    private final long totalSize;
    private final Coder<T> coder;

    public static <T> CreateSource<T> fromIterable(Iterable<T> elements, Coder<T> elemCoder)
        throws CoderException, IOException {
      ImmutableList.Builder<byte[]> allElementsBytes = ImmutableList.builder();
      long totalSize = 0L;
      for (T element : elements) {
        byte[] bytes = CoderUtils.encodeToByteArray(elemCoder, element);
        allElementsBytes.add(bytes);
        totalSize += bytes.length;
      }
      return new CreateSource<>(allElementsBytes.build(), totalSize, elemCoder);
    }

    /**
     * Create a new source with the specified bytes. The new source owns the input element bytes,
     * which must not be modified after this constructor is called.
     */
    private CreateSource(List<byte[]> elementBytes, long totalSize, Coder<T> coder) {
      super(0, elementBytes.size(), 1);
      this.allElementsBytes = ImmutableList.copyOf(elementBytes);
      this.totalSize = totalSize;
      this.coder = coder;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return totalSize;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options)
        throws IOException {
      return new BytesReader<>(this);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) throws Exception {
      return allElementsBytes.size();
    }

    @Override
    public OffsetBasedSource<T> createSourceForSubrange(long start, long end) {
      List<byte[]> primaryElems = allElementsBytes.subList((int) start, (int) end);
      long primarySizeEstimate =
          (long) (totalSize * primaryElems.size() / (double) allElementsBytes.size());
      return new CreateSource<>(primaryElems, primarySizeEstimate, coder);
    }

    @Override
    public long getBytesPerOffset() {
      if (allElementsBytes.size() == 0) {
        return 1L;
      }
      return Math.max(1, totalSize / allElementsBytes.size());
    }

    private static class BytesReader<T> extends OffsetBasedReader<T> {
      private int index;
      /**
       * Use an optional to distinguish between null next element (as Optional.absent()) and no next
       * element (next is null).
       */
      @Nullable
      private Optional<T> next;

      public BytesReader(CreateSource<T> source) {
        super(source);
        index = -1;
      }

      @Override
      @Nullable
      public T getCurrent() throws NoSuchElementException {
        if (next == null) {
          throw new NoSuchElementException();
        }
        return next.orNull();
      }

      @Override
      public void close() throws IOException {}

      @Override
      protected long getCurrentOffset() {
        return index;
      }

      @Override
      protected boolean startImpl() throws IOException {
        return advanceImpl();
      }

      @Override
      public synchronized CreateSource<T> getCurrentSource() {
        return (CreateSource<T>) super.getCurrentSource();
      }

      @Override
      protected boolean advanceImpl() throws IOException {
        CreateSource<T> source = getCurrentSource();
        if (index + 1 >= source.allElementsBytes.size()) {
          next = null;
          return false;
        }
        index++;
        next =
            Optional.fromNullable(
                CoderUtils.decodeFromByteArray(source.coder, source.allElementsBytes.get(index)));
        return true;
      }
    }
  }

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Configuration conf = new Configuration();

    BoundedSource<KV<String, Integer>> source = CreateSource.fromIterable(
        ImmutableList.of(KV.of("k1", 10), KV.of("k2", 2)),
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    conf.set(
        "source",
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(source)));

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MapReduceWordCount.class);
    job.setInputFormatClass(BeamInputFormat.class);
    job.setMapperClass(BeamMapper.class);
    //job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(IntSumReducer.class);
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(IntWritable.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    job.setOutputFormatClass(NullOutputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
