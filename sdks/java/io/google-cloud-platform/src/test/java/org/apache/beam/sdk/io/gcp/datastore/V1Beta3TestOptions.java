package org.apache.beam.sdk.io.gcp.datastore;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipelineOptions;

import javax.annotation.Nullable;

/**
 * V1Beta3 Datastore related pipeline options
 */
public interface V1Beta3TestOptions extends TestPipelineOptions {
  @Description("Project ID to read from datastore")
  @Validation.Required
  String getProject();
  void setProject(String value);

  @Description("Datastore Entity kind")
  String getKind();
  void setKind(String value);

  @Description("Datastore Namespace")
  String getNamespace();
  void setNamespace(@Nullable String value);
}
