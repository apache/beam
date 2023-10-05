# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
from dataclasses import dataclass
from typing import List, Tuple

import argparse
import requests
import re

from models import ComplexityEnum

SCIO_REPOSITORY = "https://raw.githubusercontent.com/spotify/scio/"
SCIO_BRANCH = "main"


@dataclass
class ScioExampleTag:
    filepath: str
    name: str
    description: str
    multifile: bool
    pipeline_options: str
    default_example: bool
    context_line: int
    categories: List[str]
    complexity: ComplexityEnum
    tags: List[str]


SCIO_EXAMPLES: List[ScioExampleTag] = [
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/DebuggingWordCount.scala",
        name="DebuggingWordCount",
        description="Word Count Example with Assertions.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Debugging", "Filtering", "Options", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
        ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/MetricsExample.scala",
        name="MetricsExample",
        description="Metrics example.",
        multifile=False,
        pipeline_options="",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/MinimalWordCount.scala",
        name="MinimalWordCount",
        description="An example that counts words in Shakespeare's works.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=True,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.BASIC,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/SafeFlatMapExample.scala",
        name="SafeFlatMapExample",
        description="SafeFlatMap usage",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/StatefulExample.scala",
        name="StatefulExample",
        description="Stateful Processing.",
        multifile=False,
        pipeline_options="",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/complete/TfIdf.scala",
        name="TfIdf",
        description="Compute TF-IDF from a Text Corpus.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/TsvExample.scala",
        name="TsvExampleWrite",
        description="Reading and writing tsv data.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/WordCount.scala",
        name="WordCount",
        description="An example that counts words in Shakespeare's works.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Options", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/WordCountScioIO.scala",
        name="WordCountScioIO",
        description="Word Count Example with Metrics and ScioIO read/write.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
    ScioExampleTag(
        filepath="scio-examples/src/main/scala/com/spotify/scio/examples/extra/WriteDynamicExample.scala",
        name="WriteDynamicExample",
        description="Demonstrates saveAsDynamic* methods.",
        multifile=False,
        pipeline_options="--output output.txt",
        default_example=False,
        context_line=1,
        categories=["Combiners", "Filtering", "IO", "Core Transforms", "Quickstart"],
        complexity=ComplexityEnum.MEDIUM,
        tags=["Example"]
    ),
]


def fetch_scala_examples() -> Tuple[ScioExampleTag, str]:
    """Fetch all Scala examples from the Scio repository."""
    urls = [(example, SCIO_REPOSITORY + "/" + SCIO_BRANCH + "/" + example.filepath) for example in SCIO_EXAMPLES]
    for example, url in urls:
        result = requests.get(url)
        if result.status_code != 200:
            print(f"Failed to fetch {url} with status code {result.status_code}, skipping")
            continue
        content = result.text
        yield example, content


def serialize_tag_to_yaml(tag: ScioExampleTag) -> str:
    """Serialize a Tag to YAML."""

    yaml = f"""beam-playground:
    name: "{tag.name}"
    description: "{tag.description}"
    multifile: {tag.multifile}
    pipeline_options: "{tag.pipeline_options}"
    default_example: {tag.default_example}
    context_line: {tag.context_line}
    categories:
"""
    for category in tag.categories:
        yaml += f"        - \"{category}\"\n"
    yaml += f"    complexity: {tag.complexity}\n"
    yaml += f"    tags:\n"
    for t in tag.tags:
        yaml += f"        - \"{t}\"\n"
    return yaml


def insert_tag_into_source(tag_yaml: str, source: str) -> str:
    """Insert the tag YAML into the source code."""

    lines = source.split("\n")
    package_line = 0
    for i, line in enumerate(lines):
        if line.startswith("package"):
            package_line = i
            break

    object_line = 0
    for i, line in enumerate(lines):
        if line.startswith("object"):
            object_line = i
            break

    tag_lines_number = tag_yaml.count("\n")
    tag_yaml = re.sub(r"context_line: \d+", f"context_line: {object_line + tag_lines_number + 3}", tag_yaml)
    tag_yaml = "// " + tag_yaml.replace("\n", "\n// ")
    tag_yaml = "\n" + tag_yaml

    # Insert the tag YAML before the package definition
    lines.insert(package_line, tag_yaml)

    return "\n".join(lines)


argparser = argparse.ArgumentParser()
argparser.add_argument("--output-dir", dest="output_dir", help="Output directory", required=True)

if __name__ == "__main__":
    args = argparser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    for tag, source in fetch_scala_examples():
        tag_yaml = serialize_tag_to_yaml(tag)
        source = insert_tag_into_source(tag_yaml, source)

        output_path = os.path.join(args.output_dir, tag.name + ".scala")

        with open(output_path, "w") as f:
            f.write(source)

        print(f"Written {output_path}")
