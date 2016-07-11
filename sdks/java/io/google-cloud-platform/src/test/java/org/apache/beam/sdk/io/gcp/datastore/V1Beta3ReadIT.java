package org.apache.beam.sdk.io.gcp.datastore;

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.deleteAllEntities;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.getDatastore;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.makeAncestorKey;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.makeEntity;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.UpsertMutationBuilder;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.V1Beta3TestWriter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;

import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.client.Datastore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

/**
 * End-to-end tests for Datastore V1Beta3.Read
 */
@RunWith(JUnit4.class)
public class V1Beta3ReadIT {

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

  @Test
  public void testE2EV1Beta3Read() throws Exception {
    Datastore datastore = getDatastore(options, options.getProject());

    // Write test entities to datastore
    V1Beta3TestWriter writer = new V1Beta3TestWriter(datastore, new UpsertMutationBuilder());
    Key ancestorKey = makeAncestorKey(options.getNamespace(), options.getKind(), ancestor);

    for(long i = 0; i < numEntities; i++) {
      Entity entity = makeEntity(i, ancestorKey, options.getKind(), options.getNamespace());
      writer.write(entity);
    }
    writer.close();

    // Read from datastore
    Query query = V1Beta3TestUtil.makeAncestorKindQuery(
        options.getKind(), options.getNamespace(), ancestor);

    V1Beta3.Read read = DatastoreIO.v1beta3().read()
        .withProjectId(options.getProject())
        .withQuery(query)
        .withNamespace(options.getNamespace());


    // Count the total number of entities
    Pipeline p = Pipeline.create(options);
    PCollection<Long> count = p
        .apply(read)
        .apply(Count.<Entity>globally());

    PAssert.thatSingleton(count).isEqualTo(numEntities);
    p.run();
  }

  @After
  public void tearDown() throws Exception {
    deleteAllEntities(options, ancestor);
  }
}
