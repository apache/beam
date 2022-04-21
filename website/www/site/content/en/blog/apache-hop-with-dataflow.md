---
title:  "Running Apache Hop visual pipelines with Google Cloud Dataflow"
date:   2022-04-22 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2022/04/22/apache-hop-with-dataflow.html
authors:
  - tbd
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


<ul style="color: red; font-weight: bold"><li>See top comment block for details on ERRORs and WARNINGs. <li>In the converted Markdown or HTML, search for inline alerts that start with >>>>  GDC alert:  for specific instances that need correction.</ul>

<p style="color: red; font-weight: bold">Links to alert messages:</p><a href="#gdcalert1">alert1</a>
<a href="#gdcalert2">alert2</a>
<a href="#gdcalert3">alert3</a>
<a href="#gdcalert4">alert4</a>
<a href="#gdcalert5">alert5</a>
<a href="#gdcalert6">alert6</a>
<a href="#gdcalert7">alert7</a>
<a href="#gdcalert8">alert8</a>
<a href="#gdcalert9">alert9</a>
<a href="#gdcalert10">alert10</a>
<a href="#gdcalert11">alert11</a>
<a href="#gdcalert12">alert12</a>
<a href="#gdcalert13">alert13</a>
<a href="#gdcalert14">alert14</a>
<a href="#gdcalert15">alert15</a>
<a href="#gdcalert16">alert16</a>
<a href="#gdcalert17">alert17</a>
<a href="#gdcalert18">alert18</a>
<a href="#gdcalert19">alert19</a>
<a href="#gdcalert20">alert20</a>
<a href="#gdcalert21">alert21</a>
<a href="#gdcalert22">alert22</a>
<a href="#gdcalert23">alert23</a>
<a href="#gdcalert24">alert24</a>
<a href="#gdcalert25">alert25</a>
<a href="#gdcalert26">alert26</a>
<a href="#gdcalert27">alert27</a>
<a href="#gdcalert28">alert28</a>
<a href="#gdcalert29">alert29</a>
<a href="#gdcalert30">alert30</a>
<a href="#gdcalert31">alert31</a>

<p style="color: red; font-weight: bold">>>>> PLEASE check and correct alert issues and delete this message and the inline alerts.<hr></p>


## Intro

Apache Hop (https://hop.apache.org/) is a visual development environment for creating data pipelines using Apache Beam. You can run your Hop pipelines in Spark, Flink or Google Cloud Dataflow.

In this post, we will see how to install Hop, and we will run a sample pipeline in the cloud with Dataflow. To follow the steps given in this post, you should have a project in Google Cloud Platform, and you should have enough permissions to create a Google Cloud Storage bucket (or to use an existing one), as well as to run Dataflow jobs.

Once you have your Google Cloud project ready, you will need to [install the Google Cloud SDK](https://cloud.google.com/sdk/docs/install) to trigger the Dataflow pipeline.

Also, don't forget to configure the Google Cloud SDK to use your account and your project.


## Setup and local execution

You can run Apache Hop as a local application, or use [the Hop web version](https://hop.incubator.apache.org/manual/latest/hop-gui/hop-web.html) from a Docker container. The instructions given in this post will work for the local application, as the authentication for Cloud Dataflow would be different if Hop is running in a container. All the rest of the instructions remain valid. The UI of Hop is exactly the same either running as a local app or in the web version.

Now it's time to download and install Apache Hop, following these [instructions](https://hop.apache.org/manual/latest/getting-started/hop-download-install.html).

For this post, I have used the binaries in the apache-hop-client package, version 1.2.0, released on March 7th, 2022.

After installing Hop, we are ready to start.

The Zip file contains a directory `config` where you will find some sample projects and some pipeline run configuration for Dataflow and other runners.

For this example, we are going to use the pipeline located in `config/projects/samples/beam/pipelines/input-process-output.hpl.`

Let's start by opening Apache Hop. In the directory where you have unzipped the client, run

`./hop/hop-gui.sh` 

(or `./hop/hop-gui.bat` if you are on Windows).

Once we are in Hop, let's open the pipeline. 

We first switch from the project `default` to the project `samples`. Locate the `projects` box in the top left corner of the window, and select the project `samples`:



<p id="gdcalert2" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image1.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert3">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image1.png "image_tooltip")


Now we click the open button:



<p id="gdcalert3" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image2.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert4">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image2.png "image_tooltip")


Select the pipeline `input-process-output.hpl` in the `beam/pipelines` subdirectory:



<p id="gdcalert4" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image3.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert5">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image3.png "image_tooltip")


You should see a graph like the following in the main window of Hop:



<p id="gdcalert5" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image4.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert6">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image4.png "image_tooltip")


This pipeline takes some customer data from a CSV file and filters out everything but the records with the column `stateCode` equal to `CA.`

Then we select only some of the columns of the file, and the result is written to Google Cloud Storage.

It is always a good idea to test the pipeline locally before submitting it to Dataflow. In Apache Hop, you can preview the output of each transform. Let's have a look at the input `Customers`.

Click in the `Customers` input transform and then in _Preview Output_ in the dialog box that opens after selecting the transform:



<p id="gdcalert6" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image5.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert7">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image5.png "image_tooltip")


Now select the option _Quick launch_ and you will see some of the input data:



<p id="gdcalert7" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image6.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert8">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image6.png "image_tooltip")


Click _Stop_ when you finish reviewing the data.

If we repeat the process right after the `Only CA` transform, we will see that all the rows have the `stateCode` column equal to `CA`.

The next transform selects only some of the columns of the input data and reorders the columns. Let's have a look. Click  the transform and then _Preview Output_:



<p id="gdcalert8" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image7.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert9">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image7.png "image_tooltip")


Then click _Quick Launch _again, and you should see output like the following:



<p id="gdcalert9" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image8.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert10">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image8.png "image_tooltip")


The column `id` is now the first, and we see only a subset of the input columns. This is how the data will look once the pipeline finishes writing the full output.


## Using the Beam Direct Runner

Let's run the pipeline. To run the pipeline, we need to specify a runner configuration. This is done through the Metadata tool of Apache Hop:



<p id="gdcalert10" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image9.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert11">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image9.png "image_tooltip")


In the `samples` project, there are already several configurations created:



<p id="gdcalert11" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image10.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert12">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image10.png "image_tooltip")


The `local` configuration is the one used to run the pipeline using Hop. For instance, this is the configuration that we used when we examined the previews of the output of different steps.

The `Direct` configuration uses the direct runner of Apache Beam. Let's examine what it looks like. There are two tabs in the Pipeline Run Configurations: main and variables. 

For the direct runner, the main tab has the following options:



<p id="gdcalert12" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image11.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert13">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image11.png "image_tooltip")


We can change the number of workers settings to match our number of CPUs, or even limit it just to 1 so the pipeline does not consume a lot of resources.

In the variables tab, we find the configuration parameters for the pipeline itself (not for the runner): \


<p id="gdcalert13" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image12.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert14">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image12.png "image_tooltip")


For this pipeline, only the `DATA_INPUT` and `DATA_OUTPUT` variables are used. The `STATE_INPUT` is used in a different example.

If you go to the Beam transforms in the input and output nodes of the pipeline, you will see how these variables are used there:



<p id="gdcalert14" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image13.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert15">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image13.png "image_tooltip")




<p id="gdcalert15" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image14.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert16">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image14.png "image_tooltip")


Since those variables are correctly set up to point to the location of data in the samples project folder, let's try to run the pipeline using the Beam Direct Runner.

For that, we need to go back to the pipeline view (arrow button just above the Metadata tool), and click the run button (the small "play" button in the toolbar). Then choose the Direct pipeline run configuration, and click the _Launch_ button:



<p id="gdcalert16" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image15.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert17">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image15.png "image_tooltip")


How do you know if the job has finished or not? You can check the logs at the bottom of the main window for that. You should see something like this:



<p id="gdcalert17" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image16.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert18">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image16.png "image_tooltip")


If we go to the location set by `DATA_OUTPUT`, in our case `config/projects/samples/beam/output`, we should see some output files there. In my case, I see these files:



<p id="gdcalert18" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image17.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert19">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image17.png "image_tooltip")


The number of files depends on the number of workers that you have set in the run configuration.

Great, so the pipeline works locally. It is time to run it in the cloud!


## Running at cloud scale with Dataflow

Let's have a look at the Dataflow Pipeline Run Configuration. Go to the metadata tool, then to Pipeline Run Configuration and select Dataflow:



<p id="gdcalert19" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image18.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert20">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image18.png "image_tooltip")


We have again the Main and the Variables tab. We will need to change some values in both. Let's start with the Variables. Click the Variables tab, and you should see the following values:



<p id="gdcalert20" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image19.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert21">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image19.png "image_tooltip")


Those are Google Cloud Storage (GCS) locations that belong to the author of that sample project. We need to change them to point to our own GCS bucket. 


## Project setup in Google Cloud

But for that, we will have to create a bucket. For the next step, you need to make sure that you have configured gcloud (the Google Cloud SDK), and that you have managed to authenticate.

To double check, run the command `gcloud config list` and check if the account and the project look correct. If they do, let's triple check and run `gcloud auth login`. That should open a tab in your web browser, to do the authentication process. Once you have done that, you can interact with your project using the SDK.

For this example, I will use the region europe-west1 of GCP. Let's create a regional bucket there. In my case, I am using the name `ihr-apache-hop-blog` for the bucket name. Choose a different name for your bucket!


```
gsutil mb -c regional -l europe-west1 gs://ihr-apache-hop-blog
```


Now let's upload the sample data to the GCS bucket, to test how the pipeline would run in Dataflow. Go to the same directory where you have all the hop files (the same directory that `hop-gui.sh` is in), and let's copy the data to GCS:


```
 gsutil cp config/projects/samples/beam/input/customers-noheader-1k.txt gs://ihr-apache-hop-blog/data/
```


Notice the final slash `/` in the path, indicating that you want to create a directory of name `data`, with all the contents.

To make sure that you have uploaded the data correctly, check the contents of that location:


```
gsutil ls gs://ihr-apache-hop-blog/data/
```


You should see the file `customer-noheader-1k.txt` in that location.

Before we continue, make sure that Dataflow is enabled in your project, and that you have a service account ready to be used with Hop. Please check the instructions given at the documentation of Dataflow, in the _[Before you begin section](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-java#before-you-begin)_ to see how to enable the API for Dataflow.

Now we need to make sure that Hop can use the necessary credentials for accessing Dataflow. In the Hop documentation, you will find that it recommends creating a service account, exporting a key for that service account, and setting the GOOGLE\_APPLICATION\_CREDENTIALS environment variable. This is also the method given in the above link.

Exporting the key of a service account is potentially dangerous, so we are going to use a different method, by leveraging the Google Cloud SDK. Run the following command:


```
gcloud auth application-default login
```


That will open a tab in your web browser asking to confirm the authentication. Once you have confirmed, any application in your system that needs to access Google Cloud Platform will use those credentials for that access.

We need also to create a service account for the Dataflow job, with certain permissions. Create the service account with 


```
​​gcloud iam service-accounts create dataflow-hop-sa
```


And now we give permissions to this service account for Dataflow:


```
gcloud projects add-iam-policy-binding ihr-hop-playground \
--member="serviceAccount:dataflow-hop-sa@ihr-hop-playground.iam.gserviceaccount.com"\
 --role="roles/dataflow.worker"
```


We also need to give additional permissions for Google Cloud Storage:


```
gcloud projects add-iam-policy-binding ihr-hop-playground \
--member="serviceAccount:dataflow-hop-sa@ihr-hop-playground.iam.gserviceaccount.com"\
 --role="roles/storage.admin"
```


Make sure that you change the project id `ihr-hop-playground` to your own project id.

Now let's give permissions to our user to impersonate that service account. For that, go to S[ervice Accounts in the Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) in your project, and click on the service account we have just created.

Click on the _Permissions_ tab and then in the _Grant Access_ button:



<p id="gdcalert21" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image20.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert22">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image20.png "image_tooltip")


Give your user the role _Service Account User_:



<p id="gdcalert22" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image21.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert23">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image21.png "image_tooltip")


You are now all set to be able to run Dataflow with that service account and your user.


## Updating the Pipeline Run Configuration

Before we can run a pipeline in Dataflow, we need to generate the JAR package for the pipeline code. For that, you have to go to the _Tools _menu (in the menu bar), and choose the option _Generate a Hop fat jar_. Click ok in the dialog, and then select a location and filename for the jar, and click _Save_:



<p id="gdcalert23" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image22.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert24">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image22.png "image_tooltip")


It will take some minutes to generate the file:



<p id="gdcalert24" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image23.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert25">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image23.png "image_tooltip")


We are ready to run the pipeline in Dataflow. Or almost :).

Go the pipeline editor, click the play button, and select _DataFlow_ as Pipeline run configuration, and then click the play button on the right side:



<p id="gdcalert25" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image24.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert26">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image24.png "image_tooltip")


That will open the Dataflow Pipeline Run Configuration, where we can change the input variables, and other Dataflow settings.

Click on the _Variables_ tab and modify only the `DATA_INPUT` and `DATA_OUTPUT` variables.



<p id="gdcalert26" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image25.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert27">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image25.png "image_tooltip")


Notice that we also need to change the filename.

Let's go now to the _Main_ tab, because there are some other options that we need to change there. We need to update:



*   Project id
*   Service account
*   Staging location
*   Region
*   Temp location
*   Fat jar file location

For project id, set your project id (the same one you see when you run `gcloud config list`).

For service account, use the address of the Service Account we have created. If you don't remember, you can find it under S[ervice Accounts in the Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts). 

For staging and temp locations, use the same bucket that we have just created. Change the bucket address in the paths, and leave the same "binaries" and "tmp" locations that are already set in the configuration.

For region, in this example we are using `europe-west1`.

Also, depending on your network configuration, you may want to check the box of "Use Public IPs?", or alternatively leave it unchecked but enable Google Private Access in the regional subnetwork for europe-west1 in your project (for more details, please see [Configuring Private Google Access | VPC](https://cloud.google.com/vpc/docs/configure-private-google-access#enabling-pga)). In this example, I will check the box for simplicity.

For the fat jar location, use the _Browse _button on the right side, and locate the JAR that we generated above. In summary, my _Main_ options look like these (your project id and locations will be different):



<p id="gdcalert27" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image26.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert28">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image26.png "image_tooltip")


You may, of course, change any other option, depending on the specific settings that might be required for your project.

When you are ready, click on the _Ok _button and then _Launch_ to trigger the pipeline.

In the logging window, you should see a line like the following:



<p id="gdcalert28" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image27.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert29">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image27.png "image_tooltip")



## Checking the job in Dataflow

If everything has gone well, you should now see a job running at https://console.cloud.google.com/dataflow/jobs.



<p id="gdcalert29" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image28.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert30">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image28.png "image_tooltip")


If for some reason the job has failed, open the failed job page, check the _Logs _at the bottom, and click the error icon to find why the pipeline has failed. It is normally because we have set some wrong option in your configuration:



<p id="gdcalert30" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image29.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert31">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image29.png "image_tooltip")


When the pipeline starts running, you should see the graph of the pipeline in the job page:



<p id="gdcalert31" ><span style="color: red; font-weight: bold">>>>>  GDC alert: inline image link here (to images/image30.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert32">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>> </span></p>


![alt_text](images/image30.png "image_tooltip")


When the job finishes, there should be a file in the output location. You can check it out with `gsutil`


```
% gsutil ls gs://ihr-apache-hop-blog/output
gs://ihr-apache-hop-blog/output/input-process-output-00000-of-00003.csv
gs://ihr-apache-hop-blog/output/input-process-output-00001-of-00003.csv
gs://ihr-apache-hop-blog/output/input-process-output-00002-of-00003.csv
```


In my case, the job has generated three files, but the actual number will vary from run to run.

Let's explore the first lines of those files: 


```
gsutil cat "gs://ihr-apache-hop-blog/output/*csv"| head
 12,wha-firstname,vnaov-name,egm-city,CALIFORNIA
 25,ayl-firstname,bwkoe-name,rtw-city,CALIFORNIA
 26,zio-firstname,rezku-name,nvt-city,CALIFORNIA
 44,rgh-firstname,wzkjq-name,hkm-city,CALIFORNIA
 135,ttv-firstname,eqley-name,trs-city,CALIFORNIA
 177,ahc-firstname,nltvw-name,uxf-city,CALIFORNIA
 181,kxv-firstname,bxerk-name,sek-city,CALIFORNIA
 272,wpy-firstname,qxjcn-name,rew-city,CALIFORNIA
 304,skq-firstname,cqapx-name,akw-city,CALIFORNIA
 308,sfu-firstname,ibfdt-name,kqf-city,CALIFORNIA
```


We can see that all the rows have CALIFORNIA as the state, that the output contains only the columns that we selected, and that the user id is the first column. The actual output you get will probably be different, as the order in which data is processed will not be the same in each run.

We have run this job with a small data sample, but we could have run the same job with an arbitrarily large input CSV. Dataflow would parallelize and process the data in chunks.


## Conclusions

 Apache Hop is a visual development environment for Beam pipelines, that allows us to run the pipelines locally, inspect the data, debug, unit test and many other capabilities. Once we are happy with a pipeline that has run locally, we can deploy the same visual pipeline in the cloud by just setting the necessary parameters for using Dataflow.

If you want to know more about Apache Hop, don't miss [the Beam Summit talk delivered by the author of Hop](https://www.youtube.com/watch?v=sZSIbcPtebI), and don't forget to check out the [getting started guide](https://hop.apache.org/manual/latest/getting-started/index.html). 
