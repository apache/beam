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
package org.apache.beam.examples.complete.cdap.salesforce;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.gson.Gson;
import io.cdap.plugin.salesforce.plugin.sink.batch.CSVRecord;
import java.util.Map;
import org.apache.beam.examples.complete.cdap.salesforce.options.CdapSalesforceSinkOptions;
import org.apache.beam.examples.complete.cdap.salesforce.transforms.FormatOutputTransform;
import org.apache.beam.examples.complete.cdap.salesforce.utils.CsvRecordCoder;
import org.apache.beam.examples.complete.cdap.salesforce.utils.PluginConfigOptionsConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TxtToCdapSalesforce} pipeline is a batch pipeline which ingests data from .txt file,
 * and outputs the resulting records to Salesforce. Salesforce parameters and input .txt file path
 * are specified by the user as template parameters. <br>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Gradle preparation
 *
 * To run this example your {@code build.gradle} file should contain the following task
 * to execute the pipeline:
 * {@code
 * task executeCdap (type:JavaExec) {
 *     mainClass = System.getProperty("mainClass")
 *     classpath = sourceSets.main.runtimeClasspath
 *     systemProperties System.getProperties()
 *     args System.getProperty("exec.args", "").split()
 * }
 * }
 *
 * This task allows to run the pipeline via the following command:
 * {@code
 * gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.salesforce.TxtToCdapSalesforce \
 *      -Dexec.args="--<argument>=<value> --<argument>=<value>"
 * }
 *
 * # Running the pipeline
 * To execute this pipeline, specify the parameters in the following format:
 * {@code
 * --username=your-user-name\
 * --password=your-password \
 * --securityToken=your-token \
 * --consumerKey=your-key \
 * --consumerSecret=your-secret \
 * --loginUrl=your-login-url \
 * --sObject=CustomObject__c \
 * --referenceName=your-reference-name \
 * --inputTxtFilePath=your-path-to-input-file \
 * --maxRecordsPerBatch=10 \
 * --maxBytesPerBatch=9999999 \
 * --operation=Insert \
 * --errorHandling=Stop on error
 * }
 *
 * By default this will run the pipeline locally with the DirectRunner. To change the runner, specify:
 * {@code
 * --runner=YOUR_SELECTED_RUNNER
 * }
 * </pre>
 */
public class TxtToCdapSalesforce {

  private static final Gson GSON = new Gson();

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(TxtToCdapSalesforce.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    CdapSalesforceSinkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CdapSalesforceSinkOptions.class);

    checkStateNotNull(options.getLocksDirPath(), "locksDirPath can not be null!");

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    run(pipeline, options);
  }

  /**
   * Runs a pipeline which reads records from .txt file and writes it to CDAP Salesforce.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(Pipeline pipeline, CdapSalesforceSinkOptions options) {
    Map<String, Object> paramsMap =
        PluginConfigOptionsConverter.salesforceBatchSinkOptionsToParamsMap(options);
    LOG.info("Starting Txt-to-Cdap-Salesforce pipeline with parameters: {}", paramsMap);

    /*
     * Steps:
     *  1) Read messages in from .txt file
     *  2) Map to KV
     *  3) Write successful records to Cdap Salesforce
     */

    pipeline
        .apply("readFromTxt", TextIO.read().from(options.getInputTxtFilePath()))
        .apply(
            MapElements.into(new TypeDescriptor<KV<NullWritable, CSVRecord>>() {})
                .via(
                    json -> {
                      CSVRecord csvRecord = GSON.fromJson(json, CSVRecord.class);
                      return KV.of(NullWritable.get(), csvRecord);
                    }))
        .setCoder(
            KvCoder.of(NullableCoder.of(WritableCoder.of(NullWritable.class)), CsvRecordCoder.of()))
        .apply(
            "writeToCdapSalesforce",
            FormatOutputTransform.writeToCdapSalesforce(paramsMap, options.getLocksDirPath()));

    return pipeline.run();
  }
}
