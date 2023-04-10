# Overview

This directory holds a utility to ease generation and storage of security
resources related to Looker.  Terraform has the ability to generate
secrets. However, these are stored in terraform state and is not recommended.

# Requirements

- [go](https://go.dev)
- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) configured with
active cluster.
- Active Looker License for jar downloads.
See https://cloud.google.com/looker/docs/downloading-looker-jar-files

# Usage

Run the following command to see usage help for the commandline utility.
