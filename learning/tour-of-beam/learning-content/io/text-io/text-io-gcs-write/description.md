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
### Writing google cloud storage file using TextIO

The `TextIO` class in the Apache Beam provides a way to read and write text files from **Google Cloud Storage** **(GCS)** in a pipeline.
To write data to a file on **GCS**, you can use the Write method and pass in the **GCS** file path as a string. Here is an example of writing a string to a text file named "**myfile.txt**" in a **GCS** bucket named "**mybucket**":

{{if (eq .Sdk "go")}}
```
p, s := beam.NewPipelineWithRoot()
s := beam.Create(p, "Hello, World!")
textio.Write(s, "gs://mybucket/myfile.txt")
if err := p.Run(); err != nil {
    fmt.Printf("Failed to execute job: %v", err)
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
Pipeline pipeline = Pipeline.create();
pipeline.apply(Create.of("Hello, World!"))
 .apply(TextIO.write().to("gs://mybucket/myfile.txt"));
pipeline.run();
```
{{end}}

{{if (eq .Sdk "python")}}
```
import apache_beam as beam

p = beam.Pipeline()
data = ['Hello, World!', 'Apache Beam']
p | beam.Create(data) | beam.io.WriteToText('gs://mybucket/myfile.txt')
p.run()
```
{{end}}

{{if (eq .Sdk "go")}}
It is important to note that in order to interact with GCS you will need to set up authentication, you can do that by setting the appropriate **GOOGLE_APPLICATION_CREDENTIALS** environment variable or using the `options.WithCredentials` method during pipeline creation.

```
options := []beam.PipelineOption{
    beam.WithCredentials(creds),
}
p, err := beam.NewPipeline(options...)
```
Where `creds` is an instance of `google.Credentials`.
{{end}}

{{if (eq .Sdk "python")}}
It is important to note that in order to interact with **GCS** you will need to set up authentication, you can do that by setting the appropriate **GOOGLE_APPLICATION_CREDENTIALS** environment variable or by using the with_options method during pipeline creation and passing gcp_project and `gcp_credentials` options.

```
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'my-project-id'
google_cloud_options.job_name = 'myjob'
google_cloud_options.staging_location = 'gs://my-bucket/staging'
google_cloud_options.temp_location = 'gs://my-bucket/temp'
google_cloud_options.region = 'us-central1'

# set credentials
credentials = GoogleCredentials.get_application_default()
```
{{end}}

{{if (eq .Sdk "java")}}
It is important to note that in order to interact with **GCS** you will need to set up authentication, need specify in the console as an additional parameter
```
--tempLocation=gs://my-bucket/temp
```
{{end}}

### Playground exercise

In the playground window, you can find an example that writes data to **GCS**. Can you modify this example to generate numbers and write them to **GCS** sorted?