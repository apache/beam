# Overview

This directory provides instruction on generating secrets and credentials
required to run a Looker instance. While this step could be automated using
terraform, such secrets would be stored in terraform state and not recommended.

The following assumes `ROOT` as the `path/to/beam` where `beam` is the directory
to which you cloned the [github.com/apache/beam](https://github.com/apache/beam)
repository.

# Requirements

- Requirements listed in 
[$ROOT/.test-infra/terraform/google-cloud-platform/looker/tool](../tool)
notably the Kubernetes cluster requirement.

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

# Generate and store a GCM Key 