# Structured Pipeline Descriptions (SPDs)

## Introduction

Structured Pipeline Descriptions, henceforth SPDs (pronounced "Speedies") are a way of 
modularizing and executing large Beam pipelines. It has been heavily inspired by and is
largely syntax-compatible with dbt Core and intends to play the same role that dbt would
play in a datawarehouse ELT stack. 

SPD is emphatically **not** intended as a programming languages, it leaves that to the SDKs.
It is intended to make it easier to use the existing Beam SDKs, particularly in a multi-language
pipeline.

## Core Concepts

Most of the core concepts in SPD are drawn from dbt Core, though their meaning is often adapted
a bit to account for the fact that SPD models execute within the Beam runner rather than within
the datawarehouse. 

### Models

In SPD `models` represent a single named PCollection that is the outcome of some number of 
transformation operations. The operations are defined through a Composite Transform from an
Expansion Service. SPD provides a convenience layer for two such services, SQL and Python, to
match the dbt user experience. An interesting are of expansion (no pun intended) would be a
standard way defining other "languages" in terms of an Expansion Service. 

### Sources

Sources are references to a Beam I/O. This differs from the way pipelines are mostly written where
`sources` would actually represent a particular I/O configuration. SPD, like dbt, uses profiles
to do the bulk of the source (and sink) configuration. Ideally this means pipelines can be written
in such a way that they can be shifted between I/Os without having to rewrite the pipeline. This is
also why SPD only supports SchemaTransforms at the SPD level (you can do what you like inside of 
your model transform)