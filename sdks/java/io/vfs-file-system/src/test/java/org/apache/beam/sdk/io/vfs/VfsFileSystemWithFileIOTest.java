package org.apache.beam.sdk.io.vfs;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.rmannibucau.rules.api.ftp.FtpFile;
import com.github.rmannibucau.rules.api.ftp.FtpServer;
import com.github.rmannibucau.rules.api.ftp.FtpServerRule;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test ensuring FileIO can be used with VFS implementations.
 */
public class VfsFileSystemWithFileIOTest implements Serializable {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER =
      new TemporaryFolder() {

        @Override
        protected void before() throws Throwable {
          super.before();
          // create some files
          IntStream.range(0, 10)
              .forEach(
                  i -> {
                    final File file = new File(getRoot(), "read/read_" + i + ".txt");
                    file.getParentFile().mkdirs();
                    try (final Writer writer = new FileWriter(file)) {
                      writer.write("value_" + i + ";beam_" + i + "\n"); // whatever
                    } catch (final IOException e) {
                      throw new IllegalStateException(e);
                    }
                  });
        }
      };
  // workaround https://github.com/apache/beam/pull/4790
  private static final CountDownLatch LATCH_singleRead = new CountDownLatch(1);
  // workaround https://github.com/apache/beam/pull/4790
  private static final CountDownLatch LATCH_ftpRead = new CountDownLatch(1);
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient FtpServerRule ftp = new FtpServerRule(this);

  @Test
  public void singleRead() throws MalformedURLException, InterruptedException {
    final String pattern =
        "vfs" + new File(TEMPORARY_FOLDER.getRoot(), "read/read_0.txt").toURI().toURL();
    PAssert.that(pipeline.apply(FileIO.match().filepattern(pattern)))
        .satisfies(
            (SerializableFunction<Iterable<MatchResult.Metadata>, Void>)
                input -> {
                  final Iterator<MatchResult.Metadata> iterator = input.iterator();
                  assertTrue(iterator.hasNext());
                  final MatchResult.Metadata next = iterator.next();
                  assertEquals(
                      "value_0;beam_0\n".getBytes(StandardCharsets.UTF_8).length, next.sizeBytes());
                  assertTrue(next.isReadSeekEfficient());
                  LATCH_singleRead.countDown();
                  return null;
                });
    pipeline.run().waitUntilFinish();
    LATCH_singleRead.await(1, TimeUnit.MINUTES);
  }

  @Test
  public void singleWrite() throws MalformedURLException, InterruptedException {
    final File target = new File(TEMPORARY_FOLDER.getRoot(), "write/write_0.txt");
    final String output = "vfs" + target.toURI().toURL();
    pipeline
        .apply(Create.of("value_0;beam_0\n"))
        .apply(TextIO.write().to(output).withoutSharding() /*no suffix in name*/);
    pipeline.run().waitUntilFinish();
    int maxRetries = 60 * 4;
    for (int i = 0; i < maxRetries; i++) {
      try {
        assertTrue(target.exists());
        assertEquals(
            "value_0;beam_0\n", Files.lines(target.toPath()).collect(Collectors.joining("\n")));
        return; // done!
      } catch (final AssertionError ae) {
        if (i == maxRetries - 1) {
          throw ae;
        }
        Thread.sleep(250);
      } catch (final IOException e) {
        fail(e.getMessage());
      }
    }
    fail("write_0.txt was not written as expected");
  }

  @Test
  @FtpServer(files = @FtpFile(name = "foo.txt", content = "value_0;beam_0\n"))
  public void ftpRead() throws InterruptedException {
    final String pattern =
        format(
            "vfsftp://test:testpwd@localhost:%s/foo.txt",
            System.getProperty("com.github.rmannibucau.rules.ftp.port"));
    PAssert.that(pipeline.apply(FileIO.match().filepattern(pattern)))
        .satisfies(
            (SerializableFunction<Iterable<MatchResult.Metadata>, Void>)
                input -> {
                  final Iterator<MatchResult.Metadata> iterator = input.iterator();
                  assertTrue(iterator.hasNext());
                  final MatchResult.Metadata next = iterator.next();
                  assertEquals(
                      "value_0;beam_0\n".getBytes(StandardCharsets.UTF_8).length, next.sizeBytes());
                  assertTrue(next.isReadSeekEfficient());
                  LATCH_ftpRead.countDown();
                  return null;
                });
    pipeline.run().waitUntilFinish();
    LATCH_ftpRead.await(1, TimeUnit.MINUTES);
  }
}
