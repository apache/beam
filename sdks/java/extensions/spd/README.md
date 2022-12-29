# Structured Pipeline Descriptions (SPDs)

## Introduction

Structured Pipeline Descriptions, henceforth SPDs (pronounced "Speedies") are a way of 
modularizing and executing large Beam pipelines. It has been heavily inspired by and is
largely syntax-compatible with dbt Core and intends to play a similar role for data pipeline applications.

SPD is emphatically **not** intended as a programming language and relies on Beam SDKs (SQL, Python, Java, Go) 
to provide business logic implementation. It is aimed at structuring, testing and executing that business logic
in a pipeline, particularly Beam's multi-language pipelines. 

## Core Concepts

As our aim is to make SPD familiar to people coming from dbt we share many of the same core concepts,
though modified for the data pipeline context.


### Models

In SPD `models` represent a single named PCollection. Generally, this will be the expansion 
of a composite transform in the underlying Beam transform. This is similar to the Beam SQL
`table` model, which serves as the underlying implementation for models in SPD. 

#### Supported "Languages"

* **Beam SQL** is used to implement dbt-style SQL queries and we support a subset of the Jinja macro language that allows things like `ref` and `source` to work as expected. At the moment we are using the Calcite engine, but this could be configurable by the user.
* **Python** is implemented as an external transform calling to Beam's Python SDK. Like dbt you specify a function in a `.py` file that takes in a Dataframe as an argument. It doesn't actually matter what this function is named and unlike dbt the Dataframe argument comes from a `ref` or `source` macro included as a comment. 
* **Typescript** Not yet implemented. Support here should be similar to Python.
* **Go** Not yet implemented and unclear what form it will take. Might end up being part of the "generic expansion service" model.
* **Javascript** (or other ScriptEngine languages). Not a real language/SDK but intended as a convient way of defining composite transforms and simple UDFs without resorting to writing a native Java PTransform just to do expansion.

#### Materialization

Like dbt, models can be materialized. At the moment we support `ephemeral` and `table` materialization
for models. The former is the default and means that the model only exists as a PCollection. When 
materialized as a `table` data will egress via a WriteIO as defined in the current profile. For example,
if the output of SPD was defined to be, say, Pubsub a writeIO would be created.

We have also considered a `view` materialization that would create the appropriate PCollectionView for use
as a side input, but it's not clear that this is necessary/desirable.

### Sources

Sources in SPD are essentially a special case of `model`, also representing a PCollection but always one supplied
by a ReadIO and configured from the pipeline's profile. Under the hood we are using Beam SQLs `TableProviders` to 
provide read IO support. 

### Schemas

One of the differences between dbt and SPD is that we often require schemas with data types (`columns:`) to be supplied. In many
situations this is not technically necessary as we could fetch the true schema from the underlying storage system (BigQuery, JDBC, etc). We have intentionally chosen not to do that to provide better support for CI/CD systems.

One of the problems you often encounter with data pipelines are incompatible changes in upstream data sources. By specifying the expected schema within the SPD
pipeline we can validate that the external schema is still compatible with the expected schema of the pipeline itself, which should allow CI systems to implement
presubmit tests for schema migrations.

## Profiles 

SPD defines profiles in a way that is largely compatible with dbt. At a minimum this means that profiles should include a `target` as well as 
appropriate `outputs` settings for each project/target. This acts like dbt in that sources and materializations will both use the defined 
connector for their operations. To support the data motion use case more common to data pipelines, SPD also allows the user to specify a separate `inputs`
block which uses the same format as `outputs.` This will cause `sources` to use the `inputs` connector and materializations to use the `outputs` connector.

SPD also adds a `runners` block which allows profiles to configure where the pipeline will execute for a given target. It also allows the user to specify 
which expansion servers to use for a given target. This is intended to make the testing and deployment cycle easier by using, say, a local Flink for development
and then deploying to Dataflow for production. As with dbt secrets are provided separately to allow profiles to be version controlled.