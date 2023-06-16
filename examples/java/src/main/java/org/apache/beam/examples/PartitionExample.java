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
package org.apache.beam.examples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: PartitionPercentile
//   description: Demonstration of Partition transform usage.
//   multifile: false
//   default_example: false
//   context_line: 58
//   categories:
//     - Core Transforms
//     - Coders
//   complexity: MEDIUM
//   tags:
//     - transforms
//     - partitions
//     - coders

public class PartitionExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    CoderProvider coderProvider =
        CoderProviders.fromStaticMethods(Student.class, StudentCoder.class);
    pipeline.getCoderRegistry().registerCoderProvider(coderProvider);
    // [START main_section]
    // Provide an int value with the desired number of result partitions, and a
    // PartitionFn that represents the
    // partitioning function. In this example, we define the PartitionFn in-line.
    // Returns a PCollectionList
    // containing each of the resulting partitions as individual PCollection
    // objects.
    PCollection<Student> students =
        pipeline.apply(
            Create.of(
                Student.of("Amy", 88),
                Student.of("Bob", 87),
                Student.of("Chris", 49),
                Student.of("Dylan", 62),
                Student.of("Ellen", 78),
                Student.of("Francis", 53),
                Student.of("Gagan", 43),
                Student.of("Holly", 59),
                Student.of("Irene", 22),
                Student.of("Jack", 19),
                Student.of("Kelly", 74),
                Student.of("Loris", 43),
                Student.of("Megan", 13),
                Student.of("Nemo", 97),
                Student.of("Omar", 50),
                Student.of("Patty", 58),
                Student.of("Qi", 31),
                Student.of("Raj", 40),
                Student.of("Sandy", 20),
                Student.of("Tina", 0),
                Student.of("Uma", 97),
                Student.of("Vicky", 41),
                Student.of("Wendy", 62),
                Student.of("Xin", 59),
                Student.of("Yvonne", 57),
                Student.of("Zane", 89)));
    // Split students up into 10 partitions, by percentile:
    PCollectionList<Student> studentsByPercentile =
        students.apply(
            Partition.of(
                10,
                new Partition.PartitionFn<Student>() {
                  @Override
                  public int partitionFor(Student student, int numPartitions) {
                    return student.getPercentile() // 0..99
                        * numPartitions
                        / 100;
                  }
                }));

    // You can extract each partition from the PCollectionList using the get method,
    // as follows:
    PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
    // [END main_section]
    fortiethPercentile.apply(ParDo.of(new LogOutput<>("Fortieth percentile: ")));
    pipeline.run();
  }

  static class Student {
    private Student() {}

    private String name = "";
    private int percentile;

    public String getName() {
      return name;
    }

    public int getPercentile() {
      return percentile;
    }

    public static Student of(String name, int percentile) {
      Student student = new Student();
      student.name = name;
      student.percentile = percentile;
      return student;
    }

    @Override
    public String toString() {
      return name + " (" + percentile + ")";
    }
  }

  static class StudentCoder extends Coder<Student> {
    public static StudentCoder of() {
      return new StudentCoder();
    }

    @Override
    public void encode(Student student, OutputStream outStream) throws IOException {
      String serializableStudent = student.getName() + "," + student.getPercentile();
      outStream.write(serializableStudent.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Student decode(InputStream inStream) throws IOException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();

      int nRead;
      byte[] data = new byte[1024];

      while ((nRead = inStream.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, nRead);
      }

      String serializableStudent = buffer.toString(StandardCharsets.UTF_8.name());
      String[] fields = serializableStudent.split(",", -1);
      return Student.of(fields[0], Integer.parseInt(fields[1]));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {}
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
    private final String prefix;

    public LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }
}
