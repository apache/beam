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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assembles the list of {@link PipelineLineageVisitor}s, guarding each IO-specific visitor with a
 * classpath check so this extension works without the corresponding IO module — the same pattern as
 * the Flink integration's {@code VisitorFactoryImpl} and {@code ClassUtils}.
 */
class VisitorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(VisitorFactory.class);

  private final List<PipelineLineageVisitor> visitors = new ArrayList<>();

  VisitorFactory() {
    if (hasClass("org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource")) {
      visitors.add(new PubsubLineageVisitor());
    }
    if (hasClass("org.apache.beam.sdk.io.iceberg.IcebergIO")
        && hasClass("org.apache.iceberg.catalog.TableIdentifier")) {
      visitors.add(new IcebergLineageVisitor());
    }
    visitors.add(new LineageProviderVisitor());
  }

  List<DatasetIdentifier> extractInputs(PTransform<?, ?> transform) {
    List<DatasetIdentifier> result = new ArrayList<>();
    for (PipelineLineageVisitor visitor : visitors) {
      try {
        if (visitor.isDefinedAt(transform)) {
          result.addAll(visitor.applyInputs(transform));
        }
      } catch (RuntimeException | NoClassDefFoundError e) {
        LOG.warn("Lineage visitor {} failed", visitor.getClass().getSimpleName(), e);
      }
    }
    return result;
  }

  List<DatasetIdentifier> extractOutputs(PTransform<?, ?> transform) {
    List<DatasetIdentifier> result = new ArrayList<>();
    for (PipelineLineageVisitor visitor : visitors) {
      try {
        if (visitor.isDefinedAt(transform)) {
          result.addAll(visitor.applyOutputs(transform));
        }
      } catch (RuntimeException | NoClassDefFoundError e) {
        LOG.warn("Lineage visitor {} failed", visitor.getClass().getSimpleName(), e);
      }
    }
    return result;
  }

  private static boolean hasClass(String className) {
    try {
      Class.forName(className, false, VisitorFactory.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return false;
    }
  }

  /** Picks up transforms that implement the public {@link LineageProvider} extension point. */
  private static class LineageProviderVisitor extends PipelineLineageVisitor {
    @Override
    boolean isDefinedAt(PTransform<?, ?> transform) {
      return transform instanceof LineageProvider;
    }

    @Override
    List<DatasetIdentifier> applyInputs(PTransform<?, ?> transform) {
      return ((LineageProvider) transform).getInputDatasets();
    }

    @Override
    List<DatasetIdentifier> applyOutputs(PTransform<?, ?> transform) {
      return ((LineageProvider) transform).getOutputDatasets();
    }
  }
}
