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
package org.apache.beam.sdk.io.cdap;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded/bounded sources and sinks from <a
 * href="https://github.com/data-integrations">CDAP</a> plugins.
 */
@SuppressWarnings("UnusedVariable")
public class CdapIO {

  private static final Logger LOG = LoggerFactory.getLogger(CdapIO.class);

  public static <K, V> Read<K, V> read() {
    return new AutoValue_CdapIO_Read.Builder<K, V>().build();
  }

  /** A {@link PTransform} to read from CDAP source. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes", "unchecked", "UnnecessaryParentheses", "UnusedVariable"})
  public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

    abstract @Nullable PluginConfig getPluginConfig();

    abstract @Nullable CdapPlugin getCdapPlugin();

    abstract @Nullable Class<K> getKeyClass();

    abstract @Nullable Class<V> getValueClass();

    abstract Builder<K, V> toBuilder();

    @Experimental(Experimental.Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder<K, V> {

      abstract Builder<K, V> setPluginConfig(PluginConfig config);

      abstract Builder<K, V> setCdapPlugin(CdapPlugin plugin);

      abstract Builder<K, V> setKeyClass(Class<K> keyClass);

      abstract Builder<K, V> setValueClass(Class<V> valueClass);

      abstract Read<K, V> build();
    }

    public Read<K, V> withCdapPluginClass(Class<?> cdapPluginClass) {

      // TODO: build CdapPlugin correctly
      CdapPlugin plugin = new CdapPlugin(cdapPluginClass);

      return toBuilder().setCdapPlugin(plugin).build();
    }

    public Read<K, V> withPluginConfig(PluginConfig pluginConfig) {
      return toBuilder().setPluginConfig(pluginConfig).build();
    }

    public Read<K, V> withKeyClass(Class<K> keyClass) {
      return toBuilder().setKeyClass(keyClass).build();
    }

    public Read<K, V> withValueClass(Class<V> valueClass) {
      return toBuilder().setValueClass(valueClass).build();
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin input) {
      checkArgument(getPluginConfig() != null, "withPluginConfig() is required");
      checkArgument(getCdapPlugin() != null, "withCdapPluginClass() is required");
      checkArgument(getKeyClass() != null, "withKeyClass() is required");
      checkArgument(getValueClass() != null, "withValueClass() is required");

      getCdapPlugin().setKeyClass(getKeyClass());
      getCdapPlugin().setValueClass(getValueClass());
      getCdapPlugin().prepareRun(getPluginConfig());

      if (getCdapPlugin().isUnbounded()) {
        // TODO: implement SparkReceiverIO.<~>read()
        return null;
      } else {
        Configuration hConf = getCdapPlugin().getHadoopConf();
        HadoopFormatIO.Read<K, V> readFromHadoop =
            HadoopFormatIO.<K, V>read().withConfiguration(hConf);
        return input.apply(readFromHadoop);
      }
    }
  }
}
