package org.apache.beam.sdk.io.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class CsvIOSplitFileTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void isSerializable() throws Exception {
    SerializableUtils.ensureSerializable(CsvIOSplitFile.class);
  }

  @Test
  public void givenEmptyFile_returnsEmptyOutput() throws IOException {

  }

  @Test
  public void givenNoMultilineRecords_splits() throws IOException {
    PCollection<FileIO.ReadableFile> input = filesOf("foo,8,12.1", "bar,7,11.4");
    CsvIOSplitFile underTest = new CsvIOSplitFile(csvFormat());

    // List<KV<String, String>> want = Arrays.asList(KV.of(testName.getMethodName() + ".csv", "foo,8,12.1"),
    //                                               KV.of(testName.getMethodName() + ".csv", "bar,7,11.4"));
    // PCollection<KV<String, String>> output = input.apply(underTest);
    // PAssert.that(output).containsInAnyOrder(want);
    PAssert.that(input.apply(underTest)).satisfies(itr -> {
      System.out.println(StreamSupport.stream(itr.spliterator(), false).collect(Collectors.toList()));
      return null;
    });
    pipeline.run();
  }

  @Test
  public void givenMultilineRecords_splits(){}

  @Test
  public void givenCustomRecordSeparator_splits(){}





  // create a file in temp folder with lines given
  private String fileOf(String... lines) throws IOException {
    File file = tempFolder.newFile(testName.getMethodName() + ".csv");

    Path path = Files.write(file.toPath(), Arrays.asList(lines), Charset.defaultCharset(),
        StandardOpenOption.WRITE);
    return path.toString();
  }

  private PCollection<FileIO.ReadableFile> filesOf(String ...lines) throws IOException {
    return pipeline.apply(FileIO.match().filepattern(fileOf(lines))).apply(FileIO.readMatches());
  }

  private final CSVFormat csvFormat(){
    return CSVFormat.DEFAULT.withHeader("a_string", "an_integer", "a_double").withAllowDuplicateHeaderNames(false);
  }

}
