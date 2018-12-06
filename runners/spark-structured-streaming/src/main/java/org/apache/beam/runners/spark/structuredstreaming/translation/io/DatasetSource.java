package org.apache.beam.runners.spark.structuredstreaming.translation.io;

import static com.google.common.base.Preconditions.checkArgument;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

/**
 * This is a spark structured streaming {@link DataSourceV2} implementation. As Continuous streaming
 * is tagged experimental in spark, this class does no implement {@link ContinuousReadSupport}.
 * This class is just a mix-in.
 */
public class DatasetSource<T> implements DataSourceV2, MicroBatchReadSupport {

  private final int numPartitions;
  private final Long bundleSize;
  private TranslationContext context;
  private BoundedSource<T> source;

  public DatasetSource(TranslationContext context, BoundedSource<T> source) {
    this.context = context;
    this.source = source;
    this.numPartitions = context.getSparkSession().sparkContext().defaultParallelism();
    checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");
    this.bundleSize = context.getOptions().getBundleSize();

  }

  @Override public MicroBatchReader createMicroBatchReader(Optional<StructType> schema,
      String checkpointLocation, DataSourceOptions options) {
    return new DatasetMicroBatchReader(schema, checkpointLocation, options);
  }

  /**
   * This class can be mapped to Beam {@link BoundedSource}.
   */
  private class DatasetMicroBatchReader implements MicroBatchReader {

    private Optional<StructType> schema;
    private String checkpointLocation;
    private DataSourceOptions options;

    private DatasetMicroBatchReader(Optional<StructType> schema, String checkpointLocation,
        DataSourceOptions options) {
      //TODO deal with schema and options
    }

    @Override public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
      //TODO extension point for SDF
    }

    @Override public Offset getStartOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override public Offset getEndOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override public Offset deserializeOffset(String json) {
      //TODO extension point for SDF
      return null;
    }

    @Override public void commit(Offset end) {
      //TODO no more to read after end Offset
    }

    @Override public void stop() {
    }

    @Override public StructType readSchema() {
      return null;
    }

    @Override public List<InputPartition<InternalRow>> planInputPartitions() {
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      long desiredSizeBytes;
      SparkPipelineOptions options = context.getOptions();
      try {
        desiredSizeBytes = (bundleSize == null) ?
            source.getEstimatedSizeBytes(options) / numPartitions :
            bundleSize;
        List<? extends BoundedSource<T>> sources = source.split(desiredSizeBytes, options);
        for (BoundedSource<T> source : sources) {
          result.add(new InputPartition<InternalRow>() {

            @Override public InputPartitionReader<InternalRow> createPartitionReader() {
              BoundedReader<T> reader = null;
              try {
                reader = source.createReader(options);
              } catch (IOException e) {
              }
              return new DatasetMicroBatchPartitionReader(reader);
            }
          });
        }
        return result;

      } catch (Exception e) {
        e.printStackTrace();
      }
      return result;
    }

  }

  /**
   * This class can be mapped to Beam {@link BoundedReader}
   */
  private class DatasetMicroBatchPartitionReader implements InputPartitionReader<InternalRow> {

    BoundedReader<T> reader;
    private boolean started;
    private boolean closed;

    DatasetMicroBatchPartitionReader(BoundedReader<T> reader) {
      this.reader = reader;
      this.started = false;
      this.closed = false;
    }

    @Override public boolean next() throws IOException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return !closed && reader.advance();
      }
    }

    @Override public InternalRow get() {
      List<Object> list = new ArrayList<>();
      list.add(WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp()));
      return InternalRow.apply(asScalaBuffer(list).toList());
    }

    @Override public void close() throws IOException {
      closed = true;
      reader.close();
    }
  }
}
