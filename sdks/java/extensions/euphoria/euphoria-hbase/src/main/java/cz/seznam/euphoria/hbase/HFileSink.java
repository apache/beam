/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.VoidSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.OptionalMethodBuilder;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.hadoop.output.HadoopSink;
import cz.seznam.euphoria.hadoop.output.HadoopSink.HadoopWriter;
import cz.seznam.euphoria.hbase.util.RecursiveAllPathIterator;
import cz.seznam.euphoria.shadow.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Sink to HBase using {@code HFileOutputFormat2}.
 */
public class HFileSink implements DataSink<Cell> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileSink.class);

  /**
   * User that can change hdfs permissions
   */
  public static final String HDFS_USER = "hfilesink.hdfs.user";

  /**
   * Proper owner of built HFiles
   */
  public static final String HBASE_USER = "hfilesink.hbase.user";

  /**
   * Create builder for the sink.
   * @return the sink builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements OptionalMethodBuilder<Builder> {

    private Configuration conf = HBaseConfiguration.create();
    private String table = null;
    private Path output = null;
    private boolean doBulkLoad = true;
    private Windowing<Cell, ?> windowing = null;
    private UnaryFunction<Window<?>, String> folderNaming = Object::toString;
    private final List<Update<Job>> updaters = new ArrayList<>();

    /**
     * Specify configuration to use.
     * @param conf the configuration
     * @return this
     */
    public Builder withConfiguration(Configuration conf) {
      this.conf = conf;
      return this;
    }

    /**
     * Specify target table.
     * @param table the table to read
     * @return this
     */
    public Builder withTable(String table) {
      this.table = table;
      return this;
    }

    /**
     * Set zookeeper quorum for the connection.
     * @param quorum the quorum
     * @return this
     */
    public Builder withZookeeperQuorum(String quorum) {
      updaters.add(j -> j.getConfiguration().set(
          HConstants.ZOOKEEPER_QUORUM, quorum));
      return this;
    }

    /**
     * Set parent znode for HBase zookeeper configuration.
     * @param parent the parent znode
     * @return this
     */
    public Builder withZnodeParent(String parent) {
      updaters.add(j -> j.getConfiguration().set(
          HConstants.ZOOKEEPER_ZNODE_PARENT, parent));
      return this;
    }

    /**
     * Specify output path.
     * @param path the output path
     * @return this
     */
    public Builder withOutputPath(Path path) {
      this.output = path;
      return this;
    }

    /**
     * Set windowing for the output.
     * This is needed when persisting stream output, so that the results
     * are available in some (preferably tumbling) windows.
     * @param windowing windowing to use
     * @return this
     */
    public Builder windowBy(Windowing<Cell, ?> windowing) {
      this.windowing = windowing;
      return this;
    }

    /**
     * Set windowing to be applied before bulk loading and a function to
     * convert window label to subfolder name.
     * @param <W> type of window
     * @param windowing the windowing to be applied
     * @param folderNaming function to convert window label to string
     * @return this
     */
    @SuppressWarnings("unchecked")
    public <W extends Window<W>> Builder windowBy(
        Windowing<Cell, W> windowing, UnaryFunction<W, String> folderNaming) {
      this.windowing = windowing;
      this.folderNaming = (UnaryFunction) folderNaming;
      return this;
    }

    /**
     * Whether or not to compress output HFiles.
     * @param compress compression flag
     * @return this
     */
    public Builder setCompression(boolean compress) {
      updaters.add(j -> HFileOutputFormat2.setCompressOutput(j, compress));
      return this;
    }

    /**
     * Disable call to {@link LoadIncrementalHFiles} after each window.
     * When this is disabled, the resulting HFiles would accumulate
     * in target directory and must be loaded manually.
     * @return this
     */
    public Builder disableBulkLoad() {
      this.doBulkLoad = false;
      return this;
    }

    public HFileSink build() {
      Preconditions.checkArgument(table != null, "Specify table by call to `withTable`");
      Preconditions.checkArgument(
          output != null,
          "Specify output path by call to `withOutputPath`");

      updaters.add(j -> FileOutputFormat.setOutputPath(j, output));
      return new HFileSink(
          table, doBulkLoad, windowing, folderNaming, conf, updaters);
    }

  }

  private final String tableName;
  private final boolean doBulkLoad;
  private final byte[][] endKeys;
  @Nullable
  private final Windowing<Cell, ?> windowing;
  private final UnaryFunction<Window<?>, String> folderNaming;
  private final HadoopSink<ImmutableBytesWritable, Cell> rawSink;

  private final String hdfsUser;
  private final String hbaseUser;

  @VisibleForTesting
  HFileSink(HFileSink clone) {
    this.tableName = clone.tableName;
    this.doBulkLoad = clone.doBulkLoad;
    this.endKeys = clone.endKeys;
    this.windowing = clone.windowing;
    this.folderNaming = clone.folderNaming;
    this.rawSink = clone.rawSink;
    this.hdfsUser = clone.hdfsUser;
    this.hbaseUser = clone.hbaseUser;
  }

  HFileSink(
      String tableName,
      boolean doBulkLoad,
      @Nullable Windowing<Cell, ?> windowing,
      UnaryFunction<Window<?>, String> folderNaming,
      Configuration conf,
      List<Update<Job>> updaters) {

    conf = toConf(updaters, conf);
    this.tableName = tableName;
    this.doBulkLoad = doBulkLoad;
    this.endKeys = getEndKeys(tableName, conf);
    this.windowing = windowing;
    this.folderNaming = Objects.requireNonNull(folderNaming);
    this.rawSink = new HadoopSink<>(HFileOutputFormat2.class, conf);
    this.hdfsUser = conf.get(HDFS_USER, "hdfs");
    this.hbaseUser = conf.get(HBASE_USER, "hbase");
  }

  private static Configuration toConf(List<Update<Job>> updaters, Configuration conf) {
    try {
      Configuration ret = HBaseConfiguration.create(conf);
      Job job = Job.getInstance(ret);
      for (Update<Job> u : updaters) {
        try {
          u.update(job);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
      return job.getConfiguration();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean prepareDataset(Dataset<Cell> output) {

    // initialize this inside the `keyBy` or `reduceBy` function
    final AtomicReference<ByteBuffer[]> bufferedKeys = new AtomicReference<>();

    CellComparator comparator = new CellComparator();

    Dataset<String> hfiles = ReduceByKey.of(output)
        .keyBy(c -> toRegionIdUninitialized(bufferedKeys, endKeys,
            ByteBuffer.wrap(c.getRowArray(), c.getRowOffset(), c.getRowLength())))
        // FIXME: use raw byte arrays here and rawcomparators to sort it
        // reconstruct the cell afterwards
        .reduceBy((Stream<Cell> s, Collector<String> ctx) -> {
          // this is ugly and we should make it more clear with access to key
          // via #131
          Iterator<Cell> iterator = s.iterator();
          Cell first = iterator.next();
          int id = toRegionIdUninitialized(bufferedKeys, endKeys, ByteBuffer.wrap(
              first.getRowArray(), first.getRowOffset(), first.getRowLength()));
          HadoopWriter<ImmutableBytesWritable, Cell> writer;
          writer = rawSink.openWriter(id);
          FileOutputCommitter committer = (FileOutputCommitter) writer.getOutputCommitter();
          ImmutableBytesWritable w = new ImmutableBytesWritable();
          try {
            Cell c = first;
            while (true) {
              w.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
              writer.write(Pair.of(w, c));
              if (!iterator.hasNext()) {
                break;
              }
              c = iterator.next();
            }
            writer.close();
            writer.commit();
            ctx.collect(committer.getCommittedTaskPath(
                writer.getTaskAttemptContext()).toString());
          } catch (Exception ex) {
            try {
              writer.rollback();
            } catch (IOException ex1) {
              LOG.error("Failed to rollback writer {}", id, ex1);
            }
            throw new RuntimeException(ex);
          }
        })
        .withSortedValues(comparator::compare)
        .applyIf(windowing != null, b -> b.windowBy(windowing))
        .outputValues();


    String outputDir = rawSink.getConfiguration().get(FileOutputFormat.OUTDIR);

    ReduceWindow.of(hfiles)
        .reduceBy((Stream<String> s, Collector<Void> ctx) -> {
          ctx.getWindow();
          String subdir = folderNaming.apply(ctx.getWindow());
          Path o = Strings.isNullOrEmpty(subdir)
              ? new Path(outputDir)
              : new Path(new Path(outputDir), folderNaming.apply(ctx.getWindow()));
          s.forEach(p -> moveTo(p, o));
          loadIncrementalHFiles(o);
          try {
            o.getFileSystem(rawSink.getConfiguration()).delete(o, true);
          } catch (IOException ex) {
            LOG.warn("Exception while removing the bulk-loaded directory {}", o, ex);
          }
        })
        .output()
        .persist(new VoidSink<>());

    return true;
  }

  private void moveTo(String p, Path outputDir) {
    try {
      Path source = new Path(outputDir, p);
      FileSystem fs = source.getFileSystem(rawSink.getConfiguration());
      for (FileStatus f : fs.listStatus(source)) {
        moveRecursively(fs, f.getPath(), outputDir);
        LOG.info("Moved {} to {}", f.getPath(), outputDir);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void moveRecursively(FileSystem fs, Path source, Path dest)
      throws IOException {

    Path target = new Path(dest, source.getName());
    if (!fs.exists(target)) {
      fs.mkdirs(target);
    }
    if (fs.isDirectory(target)) {
      for (FileStatus f : fs.listStatus(source)) {
        if (!f.getPath().getName().startsWith("_")
            && !f.getPath().getName().startsWith(".")) {
          if (f.isDirectory()) {
            moveRecursively(fs, f.getPath(), new Path(target, f.getPath().getName()));
          } else {
            fs.rename(f.getPath(), new Path(target, f.getPath().getName()));
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Cannot move to file " + target);
    }
  }

  @VisibleForTesting
  void loadIncrementalHFiles(Path path) {
    if (doBulkLoad) {
      try {
        LOG.info("Bulk loading path {}", path);
        Configuration conf = rawSink.getConfiguration();
        setPermissionsForHbaseUser(conf, path);
        final LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
        final TableName t = TableName.valueOf(tableName);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(t);
             RegionLocator regionLocator = conn.getRegionLocator(t);
             Admin admin = conn.getAdmin()) {

          load.doBulkLoad(path, admin, table, regionLocator);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } else {
      LOG.info("Skipping bulkloading by request.");
    }
  }

  /**
   * For the bulk load the files Hbase user (typically hbase) has to have ALL permissions to operate
   * over HFiles (for move operation). The running user then has to have permissions to delete remaining folders.
   *
   * @param conf hbase configuration
   * @param path root path for bulk load
   */
  private void setPermissionsForHbaseUser(Configuration conf, Path path) throws IOException {
    LOG.info("Granting permissions for hbase bulk load to user " + hbaseUser);
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
    ugi.doAs((PrivilegedAction<Void>) () -> ExceptionUtils.unchecked(() -> {
      FileSystem fs = path.getFileSystem(conf);
      RemoteIterator<Path> listAllIterator = new RecursiveAllPathIterator(fs, path);
      while (listAllIterator.hasNext()) {
        Path nextPath = listAllIterator.next();
        fs.setOwner(nextPath, hbaseUser, null);// move ownership to hbase user
        if (fs.isDirectory(nextPath)) { // make accessible to hbase, deletable by running user
          fs.setPermission(nextPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        }
      };
      return null;
    }));
  }

  private static int toRegionIdUninitialized(
      AtomicReference<ByteBuffer[]> endKeys,
      byte[][] bytesEndKeys,
      ByteBuffer row) {

    if (endKeys.get() == null) {
      endKeys.set(initialize(bytesEndKeys));
    }
    return toRegionId(endKeys.get(), row);
  }

  @VisibleForTesting
  static int toRegionId(ByteBuffer[] endKeys, ImmutableBytesWritable row) {
    return toRegionId(endKeys, ByteBuffer.wrap(
        row.get(), row.getOffset(), row.getLength()));
  }

  private static int toRegionId(ByteBuffer[] endKeys, ByteBuffer row) {
    int search = Arrays.binarySearch(endKeys, row);
    return search >= 0 ? search + 1 : -(search + 1);
  }

  private static ByteBuffer[] initialize(byte[][] bytesEndKeys) {
    return Arrays.stream(bytesEndKeys)
        .map(ByteBuffer::wrap)
        .toArray(l -> new ByteBuffer[bytesEndKeys.length]);
  }

  private byte[][] getEndKeys(String table, Configuration configuration) {
    try (Connection conn = ConnectionFactory.createConnection(configuration)) {
      RegionLocator locator = conn.getRegionLocator(TableName.valueOf(table));
      byte[][] ends = locator.getEndKeys();
      return Arrays.stream(ends).map(ByteBuffer::wrap)
          .filter(b -> b.remaining() > 0)
          .sorted()
          .map(b -> b.slice().array())
          .toArray(byte[][]::new);
    } catch (IOException ex) {
      LOG.error("Failed to get locations for table {}", table, ex);
      throw new RuntimeException(ex);
    }
  }


  // this is just a 'fake' sink, it delegates its functionality to `rawSink`.

  @Override
  public Writer<Cell> openWriter(int partitionId) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void commit() throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void rollback() throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  /**
   * Persist given dataset into this sink via given mapper.
   * @param <T> input datatype
   * @param input the input dataset
   * @param mapper map function for transformation of input value into {@link Cell}.
   */
  public <T> void persist(Dataset<T> input, UnaryFunction<T, Cell> mapper) {
    MapElements.of(input)
        .using(mapper)
        .output()
        .persist(this);
  }

}
