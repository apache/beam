package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for beam to spark source translation.
 */
@RunWith(JUnit4.class) public class ComplexSourceTest implements Serializable {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static File file;
  private static Pipeline pipeline;

  @BeforeClass public static void beforeClass() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    pipeline = Pipeline.create(options);
    file = TEMPORARY_FOLDER.newFile();
    OutputStream outputStream = new FileOutputStream(file);
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < 30; ++i) {
      lines.add("word" + i);
    }
    try (PrintStream writer = new PrintStream(outputStream)) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  @Test public void testBoundedSource() {
    pipeline.apply(TextIO.read().from(file.getPath()));
    pipeline.run();
  }
}
