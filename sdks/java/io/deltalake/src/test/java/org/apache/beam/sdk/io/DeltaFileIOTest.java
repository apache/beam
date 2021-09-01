package org.apache.beam.sdk.io;

import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Serializable;

/** Tests for {@link DeltaFileIO}. */
@RunWith(JUnit4.class)
public class DeltaFileIOTest implements Serializable
{
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllowEmptyExplicit() throws IOException
  {
    PAssert.that(
      p.apply("Test1",
          DeltaFileIO.snapshot()
              .filepattern(tmpFolder.getRoot().getAbsolutePath())
              .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW)
      )
    ).containsInAnyOrder();

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllowEmptyExplicit1() throws IOException {
    p.apply(
        DeltaFileIO.snapshot()
            .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")
            .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));

    p.run();
  }

}
