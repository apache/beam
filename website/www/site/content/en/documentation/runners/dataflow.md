---
type: runners
title: "Cloud Dataflow Runner"
aliases: /learn/runners/dataflow/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Using the Google Cloud Dataflow Runner

{{< language-switcher java py >}}

The Google Cloud Dataflow Runner uses the [Cloud Dataflow managed service](https://cloud.google.com/dataflow/service/dataflow-service-desc). When you run your pipeline with the Cloud Dataflow service, the runner uploads your executable code and dependencies to a Google Cloud Storage bucket and creates a Cloud Dataflow job, which executes your pipeline on managed resources in Google Cloud Platform.

The Cloud Dataflow Runner and service are suitable for large scale, continuous jobs, and provide:

* a fully managed service
* [autoscaling](https://cloud.google.com/dataflow/service/dataflow-service-desc#autoscaling) of the number of workers throughout the lifetime of the job
* [dynamic work rebalancing](https://cloud.google.com/blog/products/gcp/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow)

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the supported capabilities of the Cloud Dataflow Runner.

## Cloud Dataflow Runner prerequisites and setup {#setup}

To use the Cloud Dataflow Runner, you must complete the setup in the *Before you
begin* section of the [Cloud Dataflow quickstart](https://cloud.google.com/dataflow/docs/quickstarts)
for your chosen language.

1. Select or create a Google Cloud Platform Console project.
2. Enable billing for your project.
3. Enable the required Google Cloud APIs: Cloud Dataflow, Compute Engine,
   Stackdriver Logging, Cloud Storage, Cloud Storage JSON, and Cloud Resource
   Manager. You may need to enable additional APIs (such as BigQuery, Cloud
   Pub/Sub, or Cloud Datastore) if you use them in your pipeline code.
4. Authenticate with Google Cloud Platform.
5. Install the Google Cloud SDK.
6. Create a Cloud Storage bucket.

### Specify your dependency {#dependency}

<span class="language-java">When using Java, you must specify your dependency on the Cloud Dataflow Runner in your `pom.xml`.</span>
{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
  <version>{{< param release_latest >}}</version>
  <scope>runtime</scope>
</dependency>
{{< /highlight >}}

<span class="language-py">This section is not applicable to the Beam SDK for Python.</span>

### Self executing JAR {#self-executing-jar}

{{< paragraph class="language-py" >}}
This section is not applicable to the Beam SDK for Python.
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
In some cases, such as starting a pipeline using a scheduler such as [Apache AirFlow](https://airflow.apache.org), you must have a self-contained application. You can pack a self-executing JAR by explicitly adding the following dependency on the Project section of your pom.xml, in addition to the adding existing dependency shown in the previous section.
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
    <version>${beam.version}</version>
    <scope>runtime</scope>
</dependency>
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Then, add the mainClass name in the Maven JAR plugin.
{{< /paragraph >}}

{{< highlight java >}}
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-jar-plugin</artifactId>
  <version>${maven-jar-plugin.version}</version>
  <configuration>
    <archive>
      <manifest>
        <addClasspath>true</addClasspath>
        <classpathPrefix>lib/</classpathPrefix>
        <mainClass>YOUR_MAIN_CLASS_NAME</mainClass>
      </manifest>
    </archive>
  </configuration>
</plugin>
{{< /highlight >}}

{{< paragraph class="language-java" >}}
After running <code>mvn package -Pdataflow-runner</code>, run <code>ls target</code> and you should see (assuming your artifactId is `beam-examples` and the version is 1.0.0) the following output.
{{< /paragraph >}}

{{< highlight java >}}
beam-examples-bundled-1.0.0.jar
{{< /highlight >}}

{{< paragraph class="language-java" >}}
To run the self-executing JAR on Cloud Dataflow, use the following command.
{{< /paragraph >}}

{{< highlight java >}}
java -jar target/beam-examples-bundled-1.0.0.jar \
  --runner=DataflowRunner \
  --project=<YOUR_GCP_PROJECT_ID> \
  --region=<GCP_REGION> \
  --tempLocation=gs://<YOUR_GCS_BUCKET>/temp/ \
  --output=gs://<YOUR_GCS_BUCKET>/output
{{< /highlight >}}

## Pipeline options for the Cloud Dataflow Runner {#pipeline-options}

<span class="language-java">When executing your pipeline with the Cloud Dataflow Runner (Java), consider these common pipeline options.</span>
<span class="language-py">When executing your pipeline with the Cloud Dataflow Runner (Python), consider these common pipeline options.</span>

<div class="table-container-wrapper">
<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>

<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>dataflow</code> or <code>DataflowRunner</code> to run on the Cloud Dataflow Service.</td>
</tr>

<tr>
  <td><code>project</code></td>
  <td>The project ID for your Google Cloud Project.</td>
  <td>If not set, defaults to the default project in the current environment. The default project is set via <code>gcloud</code>.</td>
</tr>

<tr>
  <td><code>region</code></td>
  <td>The Google Compute Engine region to create the job.</td>
  <td>If not set, defaults to the default region in the current environment. The default region is set via <code>gcloud</code>.</td>
</tr>

<tr>
  <td><code>streaming</code></td>
  <td>Whether streaming mode is enabled or disabled; <code>true</code> if enabled. Set to <code>true</code> if running pipelines with unbounded <code>PCollection</code>s.</td>
  <td><code>false</code></td>
</tr>

<tr>
  <td>
    <span class="language-java"><code>tempLocation</code></span>
    <span class="language-py"><code>temp_location</code></span>
  </td>
  <td>
    <span class="language-java">Optional.</span>
    <span class="language-py">Required.</span>
    Path for temporary files. Must be a valid Google Cloud Storage URL that begins with <code>gs://</code>.
    <span class="language-java">If set, <code>tempLocation</code> is used as the default value for <code>gcpTempLocation</code>.</span>
  </td>
  <td>No default value.</td>
</tr>

<!-- Only show for Java -->
<tr class="language-java">
  <td><code>gcpTempLocation</code></td>
  <td>Cloud Storage bucket path for temporary files. Must be a valid Cloud Storage URL that begins with <code>gs://</code>.</td>
  <td>If not set, defaults to the value of <code>tempLocation</code>, provided that <code>tempLocation</code> is a valid Cloud Storage URL. If <code>tempLocation</code> is not a valid Cloud Storage URL, you must set <code>gcpTempLocation</code>.</td>
</tr>

<tr>
  <td>
    <span class="language-java"><code>stagingLocation</code></span>
    <span class="language-py"><code>staging_location</code></span>
  </td>
  <td>Optional. Cloud Storage bucket path for staging your binary and any temporary files. Must be a valid Cloud Storage URL that begins with <code>gs://</code>.</td>
  <td>
    <span class="language-java">If not set, defaults to a staging directory within <code>gcpTempLocation</code>.</span>
    <span class="language-py">If not set, defaults to a staging directory within <code>temp_location</code>.</span>
  </td>
</tr>

<!-- Only show for Python -->
<tr class="language-py">
  <td><code>save_main_session</code></td>
  <td>Save the main session state so that pickled functions and classes defined in <code>__main__</code> (e.g. interactive session) can be unpickled. Some workflows do not need the session state if, for instance, all of their functions/classes are defined in proper modules (not <code>__main__</code>) and the modules are importable in the worker.</td>
  <td><code>false</code></td>
</tr>

<!-- Only show for Python -->
<tr class="language-py">
  <td><code>sdk_location</code></td>
  <td>Override the default location from where the Beam SDK is downloaded. This value can be a URL, a Cloud Storage path, or a local path to an SDK tarball. Workflow submissions will download or copy the SDK tarball from this location. If set to the string <code>default</code>, a standard SDK location is used. If empty, no SDK is copied.</td>
  <td><code>default</code></td>
</tr>


</table>
</div>

See the reference documentation for the
<span class="language-java">[DataflowPipelineOptions](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/index.html?org/apache/beam/runners/dataflow/options/DataflowPipelineOptions.html)</span>
<span class="language-py">[`PipelineOptions`](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.PipelineOptions)</span>
interface (and any subinterfaces) for additional pipeline configuration options.

## Additional information and caveats {#additional-info}

### Monitoring your job {#monitoring}

While your pipeline executes, you can monitor the job's progress, view details on execution, and receive updates on the pipeline's results by using the [Dataflow Monitoring Interface](https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf) or the [Dataflow Command-line Interface](https://cloud.google.com/dataflow/pipelines/dataflow-command-line-intf).

### Blocking Execution {#blocking-execution}

To block until your job completes, call <span class="language-java"><code>waitToFinish</code></span><span class="language-py"><code>wait_until_finish</code></span> on the `PipelineResult` returned from `pipeline.run()`. The Cloud Dataflow Runner prints job status updates and console messages while it waits. While the result is connected to the active job, note that pressing **Ctrl+C** from the command line does not cancel your job. To cancel the job, you can use the [Dataflow Monitoring Interface](https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf) or the [Dataflow Command-line Interface](https://cloud.google.com/dataflow/pipelines/dataflow-command-line-intf).

### Streaming Execution {#streaming-execution}

If your pipeline uses an unbounded data source or sink, you must set the `streaming` option to `true`.

When using streaming execution, keep the following considerations in mind.

1. Streaming pipelines do not terminate unless explicitly cancelled by the user.
   You can cancel your streaming job from the [Dataflow Monitoring Interface](https://cloud.google.com/dataflow/pipelines/stopping-a-pipeline)
   or with the [Dataflow Command-line Interface](https://cloud.google.com/dataflow/pipelines/dataflow-command-line-intf)
   ([gcloud dataflow jobs cancel](https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/cancel)
   command).

2. Streaming jobs use a Google Compute Engine [machine type](https://cloud.google.com/compute/docs/machine-types)
   of `n1-standard-2` or higher by default. You must not override this, as
   `n1-standard-2` is the minimum required machine type for running streaming
   jobs.

3. Streaming execution [pricing](https://cloud.google.com/dataflow/pricing)
   differs from batch execution.


