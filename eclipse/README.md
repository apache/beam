# Eclipse integration

Google Cloud Dataflow SDK for Java supports the [Eclipse](https://eclipse.org/)
integrated development environment (IDE) for the development of both user
pipelines and the SDK itself. This is in addition to other supported development
environments, such as [Apache Maven](https://maven.apache.org/).

## Requirements

In addition to Eclipse, you need to install the
[M2Eclipse plugin](http://eclipse.org/m2e/) prior to importing projects.

## Development of user pipelines

We provide the [Eclipse starter project](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/tree/master/eclipse/starter)
for getting started with Cloud Dataflow in Eclipse for the development of user
pipelines and general usage of the Cloud Dataflow SDK for Java.

Start by cloning this repository or downloading its contents to your local
machine. Now, in the Eclipse IDE, choose `File` menu and then select `Import`.
In the `Import` wizard, choose `Existing Projects into Workspace` inside the
`General` group.

In the next window, set `Select root directory` to point to the location with
the contents of this repository. `Projects` list should automatically populate
with `google-cloud-dataflow-starter` project. Make sure that project is
selected and choose `Finish` to complete the import wizard.

You can now run the starter pipeline on your local machine. From the `Run` menu,
select `Run`. Choose `LOCAL` run configuration. When the execution finishes,
among other output, the console should contain text `HELLO WORLD`.

You can also run the starter pipeline on the Google Cloud Dataflow Service using
managed resources in the Google Cloud Platform. Start by following the general
Cloud Dataflow [Getting Started](https://cloud.google.com/dataflow/getting-started)
instructions. You should have a Google Cloud Platform project that has a Cloud
Dataflow API enabled, a Google Cloud Storage bucket that will serve as a
staging location, and installed and authenticated Google Cloud SDK. Now, from
the `Run` menu, select `Run configurations`. Choose `SERVICE` run configuration
inside the `Java Application` group. In the arguments tab, populate values for
`--project` and `--stagingLocation` arguments. Click `Run` to start the program.
When the execution finishes, among other output, the console should contain
`Submitted job: <job_id>` and `Job finished with status DONE` statements.

At this point, you should be ready to start making changes to
`StarterPipeline.java` and developing your own pipeline.

## Development of the SDK

You can work on the development of the Cloud Dataflow SDK itself from Eclipse.

Start by cloning this repository or downloading its contents to your local
machine. Now, in the Eclipse IDE, choose `File` menu and then select `Import`.
In the `Import` wizard, choose `Existing Maven Projects` inside the `Maven`
group. If this import source is not available, you may not have installed the
M2Eclipse plugin properly.

In the next window, set `Root Directory` to point to the location with the
contents of this repository. `Projects` list should automatically populate with
several projects including: `/pom.xml`, `sdk/pom.xml` and `examples/pom.xml`.
Make sure all projects are selected and choose `Finish` to complete the import
wizard.

In the `Package Explorer` you can now select the `src/test/java` package group
in one of the projects. From the `Run` menu, select `Run`. Choose `JUnit Test`
run configuration. This will execute all unit tests of the particular project
locally.

At this point, you should be ready to start making changes to the Cloud Dataflow
SDK for Java. Please consider sharing your improvements with the rest of the
Dataflow community by posting them as pull requests in our
[GitHub repository](https://github.com/GoogleCloudPlatform/DataflowJavaSDK).

Good luck!
