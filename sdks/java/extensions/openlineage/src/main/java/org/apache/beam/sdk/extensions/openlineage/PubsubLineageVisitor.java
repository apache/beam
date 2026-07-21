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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts Pub/Sub topics and subscriptions from {@code PubsubIO} transforms. Reads use the public
 * getters of the expanded {@link PubsubUnboundedSource}; writes reflect the package-private topic
 * getter of {@code PubsubIO.Write}. Naming follows the OpenLineage spec: namespace {@code pubsub},
 * name {@code topic:{project}:{topic}} or {@code subscription:{project}:{subscription}}.
 */
class PubsubLineageVisitor extends PipelineLineageVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubLineageVisitor.class);
  private static final String PUBSUB_WRITE_CLASS =
      "org.apache.beam.sdk.io.gcp.pubsub.PubsubIO$Write";

  @Override
  boolean isDefinedAt(PTransform<?, ?> transform) {
    return transform instanceof PubsubUnboundedSource
        || extendsClass(transform, PUBSUB_WRITE_CLASS);
  }

  @Override
  List<DatasetIdentifier> applyInputs(PTransform<?, ?> transform) {
    if (!(transform instanceof PubsubUnboundedSource)) {
      return Collections.emptyList();
    }
    PubsubUnboundedSource source = (PubsubUnboundedSource) transform;
    List<DatasetIdentifier> result = new ArrayList<>();
    PubsubClient.TopicPath topic = valueOf(source.getTopicProvider(), source.getTopic());
    if (topic != null) {
      addPath(result, "topic", topic.getPath());
    }
    PubsubClient.SubscriptionPath subscription =
        valueOf(source.getSubscriptionProvider(), source.getSubscription());
    if (subscription != null) {
      addPath(result, "subscription", subscription.getPath());
    }
    return result;
  }

  @Override
  List<DatasetIdentifier> applyOutputs(PTransform<?, ?> transform) {
    if (!extendsClass(transform, PUBSUB_WRITE_CLASS)) {
      return Collections.emptyList();
    }
    try {
      Object provider = invokeDeclared(transform, "getTopicProvider");
      if (provider instanceof ValueProvider && ((ValueProvider<?>) provider).isAccessible()) {
        Object pubsubTopic = ((ValueProvider<?>) provider).get();
        if (pubsubTopic != null) {
          Object path = invokeDeclared(pubsubTopic, "asPath");
          if (path != null) {
            List<DatasetIdentifier> result = new ArrayList<>();
            addPath(result, "topic", path.toString());
            return result;
          }
        }
      }
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.warn("Unable to extract topic from PubsubIO.Write", e);
    }
    return Collections.emptyList();
  }

  private static <T> @Nullable T valueOf(@Nullable ValueProvider<T> provider, @Nullable T direct) {
    if (direct != null) {
      return direct;
    }
    if (provider != null && provider.isAccessible()) {
      return provider.get();
    }
    return null;
  }

  /** Converts "projects/{project}/topics/{topic}" into the OpenLineage spec name. */
  private static void addPath(List<DatasetIdentifier> result, String kind, String path) {
    String[] parts = path.split("/", -1);
    if (parts.length >= 4) {
      result.add(new DatasetIdentifier(kind + ":" + parts[1] + ":" + parts[3], "pubsub"));
    } else {
      result.add(new DatasetIdentifier(kind + ":" + path, "pubsub"));
    }
  }

  private static @Nullable Object invokeDeclared(Object target, String methodName)
      throws ReflectiveOperationException {
    Class<?> cls = target.getClass();
    while (cls != null) {
      try {
        Method method = cls.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
      } catch (NoSuchMethodException e) {
        cls = cls.getSuperclass();
      }
    }
    return null;
  }
}
