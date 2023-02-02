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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.applyCommonFileIOWriteFeatures;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.nio.charset.Charset;
import java.util.Optional;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.XmlConfiguration;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/** A {@link FileWriteSchemaTransformFormatProvider} for XML format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class XmlWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  private static final String SUFFIX = String.format(".%s", XML);

  @Override
  public String identifier() {
    return XML;
  }

  /**
   * Builds a {@link PTransform} that transforms a {@link Row} {@link PCollection} into result
   * {@link PCollection} file names written using {@link XmlIO.Sink} and {@link FileIO.Write}.
   */
  @Override
  public PTransform<PCollection<Row>, PCollection<String>> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {
    return new PTransform<PCollection<Row>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Row> input) {

        PCollection<XmlRowAdapter> xml =
            input.apply(
                "Row to XML",
                MapElements.into(TypeDescriptor.of(XmlRowAdapter.class)).via(new RowToXmlFn()));

        XmlConfiguration xmlConfig = xmlConfiguration(configuration);

        checkArgument(!Strings.isNullOrEmpty(xmlConfig.getCharset()), "charset must be specified");
        checkArgument(
            !Strings.isNullOrEmpty(xmlConfig.getRootElement()), "rootElement must be specified");

        Charset charset = Charset.forName(xmlConfig.getCharset());

        XmlIO.Sink<XmlRowAdapter> sink =
            XmlIO.sink(XmlRowAdapter.class)
                .withCharset(charset)
                .withRootElement(xmlConfig.getRootElement());

        FileIO.Write<Void, XmlRowAdapter> write =
            FileIO.<XmlRowAdapter>write()
                .to(configuration.getFilenamePrefix())
                .via(sink)
                .withSuffix(SUFFIX);

        write = applyCommonFileIOWriteFeatures(write, configuration);

        return xml.apply("Write XML", write)
            .getPerDestinationOutputFilenames()
            .apply("perDestinationOutputFilenames", Values.create());
      }
    };
  }

  /** A {@link SerializableFunction} for converting {@link Row}s to {@link XmlRowAdapter}s. */
  static class RowToXmlFn implements SerializableFunction<Row, XmlRowAdapter> {

    @Override
    public XmlRowAdapter apply(Row input) {
      XmlRowAdapter result = new XmlRowAdapter();
      result.wrapRow(input);
      return result;
    }
  }

  private static XmlConfiguration xmlConfiguration(
      FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for parameter of requireNonNull
    Optional<XmlConfiguration> result = Optional.ofNullable(configuration.getXmlConfiguration());
    checkState(result.isPresent());
    return result.get();
  }
}
