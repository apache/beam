# Community contributions

This directory hosts a wide variety of community contributions that may be
useful to other users of
[Google Cloud Dataflow](https://cloud.google.com/dataflow/),
but may not be appropriate or ready yet for inclusion into the
[mainline SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/) or a
separate Google-maintained artifact.

## Organization

Each subdirectory represents a logically separate and independent module.
Preferably, the code is hosted directly in this repository. When appropriate, we
are also open to linking external repositories via
[`submodule`](http://git-scm.com/docs/git-submodule/) functionality within Git.

While we are happy to host individual modules to provide additional value to all
Cloud Dataflow users, the modules are _maintained solely by their respective
authors_. We will make sure that modules are related to Cloud Dataflow, that
they are distributed under the same license as the mainline SDK, and provide
some guidance to the authors to make the quality as high as possible.

We __cannot__, however, provide _any_ guarantees about correctness,
compatibility, performance, support, test coverage, maintenance or future
availability of individual modules hosted here.

## Process

In general, we recommend to get in touch with us through the issue tracker
first. That way we can help out and possibly guide you. Coordinating up front
makes it much easier to avoid frustration later on.

We welcome pull requests with a new module from everyone. Every module must be
related to Cloud Dataflow and must have an informative README.md file. We will
provide general guidance, but usually won't be reviewing the module in detail.
We reserve the right to refuse acceptance to any module, or remove it at any
time in the future.

We also welcome improvements to an existing module from everyone. We'll often
wait for comments from the primary author of the module before merging a pull
request from a non-primary author.

As the module matures, we may choose to pull it directly into the mainline SDK
or promote it to a Google-managed artifact.

## Licensing

We require all contributors to sign the Contributor License Agreement, exactly
as we require for any contributions to the mainline SDK. More information is
available in our [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/CONTRIBUTING.md)
file.

_Thank you for your contribution to the Cloud Dataflow community!_
