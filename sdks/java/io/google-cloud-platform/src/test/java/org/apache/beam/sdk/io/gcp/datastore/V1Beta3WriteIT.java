package org.apache.beam.sdk.io.gcp.datastore;

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.deleteAllEntities;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.CreateEntityFn;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.V1Beta3TestReader;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.client.Datastore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

/**
 * End-to-end tests for Datastore V1Beta3.Write.
 */
@RunWith(JUnit4.class)
public class V1Beta3WriteIT {
  private V1Beta3TestOptions options;
  private String ancestor;
  private final long numEntities = 1000;

  @Before
  public void setup() {
    PipelineOptionsFactory.register(V1Beta3TestOptions.class);
    options = TestPipeline.testingPipelineOptions()
        .as(V1Beta3TestOptions.class);

    ancestor = UUID.randomUUID().toString();
  }

  /**
   * An end-to-end test for {@link V1Beta3.Write}.
   *
   * Write some test entities to datastore through a dataflow pipeline.
   * Read and count all the entities. Verify that the count matches the
   * number of entities written.
   */
  @Test
  public void testE2EV1Beta3Write() throws Exception {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(CountingInput.upTo(numEntities))
        .apply(ParDo.of(new CreateEntityFn(
            options.getKind(), options.getNamespace(), ancestor)))
        .apply(DatastoreIO.v1beta3().write().withProjectId(options.getProject()));

    p.run();

    // Read from datastore.
    Datastore datastore = V1Beta3TestUtil.getDatastore(options, options.getProject());
    Query query = V1Beta3TestUtil.makeAncestorKindQuery(
        options.getKind(), options.getNamespace(), ancestor);

    V1Beta3TestReader reader = new V1Beta3TestReader(datastore, query, options.getNamespace());

    long numEntitiesRead = 0;
    while (reader.advance()) {
      reader.getCurrent();
      numEntitiesRead++;
    }

    assertEquals(numEntitiesRead, numEntities);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllEntities(options, ancestor);
  }
}
