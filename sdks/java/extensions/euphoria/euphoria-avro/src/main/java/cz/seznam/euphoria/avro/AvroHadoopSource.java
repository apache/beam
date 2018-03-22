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
package cz.seznam.euphoria.avro;

import cz.seznam.euphoria.hadoop.input.HadoopFileSource;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.google.common.base.Preconditions;

/** Read data from Avro format */
public class AvroHadoopSource extends HadoopFileSource<AvroKey<Record>, NullWritable> {

  private static final long serialVersionUID = -714547320436003072L;

  /** @param conf standard Hadoop configuration, all data set during the build phase */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private AvroHadoopSource(Configuration conf) {
    super((Class) AvroKey.class, NullWritable.class, AvroKeyInputFormat.class, conf);
  }

  public static AvroHadoopSourceBuilder of() {
    return new AvroHadoopSourceBuilder();
  }

  public static class AvroHadoopSourceBuilder {
    private Path path;
    private Configuration conf;
    private String schema;

    AvroHadoopSourceBuilder() {
    }

    public AvroHadoopSourceBuilder path(Path path) {
      this.path = path;
      return this;
    }

    public AvroHadoopSourceBuilder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public AvroHadoopSourceBuilder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public AvroHadoopSource build() {
      if (conf == null) {
        conf = new Configuration();
      } else {
        conf = new Configuration(conf);
      }
      Preconditions.checkArgument(path != null,"Input path has to be specified");
      conf.set(FileInputFormat.INPUT_DIR, path.toString());
      if (schema != null) {
        conf.set("avro.schema.input.key", schema);
      }
      return new AvroHadoopSource(conf);
    }

    public String toString() {
      return "AvroHadoopSource.AvroHadoopSourceBuilder(path=" + this.path + ", conf=" + this.conf + ", schema=" + this.schema + ")";
    }
  }
}
