# Overview

This directory provides instruction on downloading and storing Looker jars
in a Google Cloud storage bucket. While this step could be automated using
terraform, it requires a Looker license key that is stored in terraform
state file.

The following assumes `ROOT` as the `path/to/beam` where `beam` is the directory
to which you cloned the [github.com/apache/beam](https://github.com/apache/beam)
repository.

# Requirements

- Google Cloud storage bucket provisioned from [01.setup](../01.setup)
- Requirements listed in 
[$ROOT/.test-infra/terraform/google-cloud-platform/looker/tool](../tool).

# Navigate to the tool directory

[$ROOT/.test-infra/terraform/google-cloud-platform/looker/tool](../tool)
contains a command-line utility to download jars to your local machine.

```
cd $ROOT/.test-infra/terraform/google-cloud-platform/looker/tool
```

# Validate the working command-line utility

```
go run ./cmd
```

You should see something similar to:

```
Utilities to help with Looker installations

Usage:
  lookerctl [command]
```

# Validate valid Looker License credentials

Run the following command to validate your Looker License credentials.
See https://cloud.google.com/looker/docs/downloading-looker-jar-files for
more details on the Looker License.

You will be prompted for the license and associated email.

```
go run ./cmd jars metadata
```

# Download Looker jar files

Set jar directory for re-usability in the following commands.

```
LOOKER_JARS=path/to/folder
```

Run the following command to download jar files to a target directory. You
will be prompted as before for the license and associated email.

```
go run ./cmd jars download $LOOKER_JARS
```

# Rename Looker jar files

Rename jar files, removing version information.

The downloaded jars name contains version information, for example:
```
looker-23.4.36.jar  looker-dependencies-23.4.36.jar
```

The [startup script](../04.build_image/bin/looker.sh) assumes the names:
`looker.jar` and `looker-dependencies.jar`, respectively.
Therefore, rename the jar files to match this expected format.

# Copy Looker Jar files to Google Cloud Storage

The [image build configuration](../04.build_image/cloudbuild.yaml) downloads
the jar files in the Google Cloud storage bucket as part of the image build
process.

## Acquire the Google Cloud storage bucket created in during
[01.setup](../01.setup).

Navigate to the [looker](../) directory.

```
cd $ROOT/.test-infra/terraform/google-cloud-platform/looker
```

Acquire the Google Cloud storage bucket name.
```
BUCKET=$(terraform -chdir=01.setup output -raw looker_jars_bucket
```

## Copy jars into the Google Cloud storage bucket

```
gsutil cp "$LOOKER_JARS/*.jar" "gs://$BUCKET"
```
