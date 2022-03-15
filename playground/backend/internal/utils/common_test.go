// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"testing"
)

const (
	sourceDir         = "sourceDir"
	fileName          = "file.txt"
	fileContent       = "content"
	javaFileName      = "javaFileName.java"
	pythonExampleName = "wordCount.py"
	wordCountPython   = "import argparse\nimport logging\nimport re\n\nimport apache_beam as beam\nfrom apache_beam.io import ReadFromText\nfrom apache_beam.io import WriteToText\nfrom apache_beam.options.pipeline_options import PipelineOptions\nfrom apache_beam.options.pipeline_options import SetupOptions\n\n\nclass WordExtractingDoFn(beam.DoFn):\n  \"\"\"Parse each line of input text into words.\"\"\"\n  def process(self, element):\n    \"\"\"Returns an iterator over the words of this element.\n\n    The element is a line of text.  If the line is blank, note that, too.\n\n    Args:\n      element: the element being processed\n\n    Returns:\n      The processed element.\n    \"\"\"\n    return re.findall(r'[\\w\\']+', element, re.UNICODE)\n\n\ndef run(argv=None, save_main_session=True):\n  \"\"\"Main entry point; defines and runs the wordcount pipeline.\"\"\"\n  parser = argparse.ArgumentParser()\n  parser.add_argument(\n      '--input',\n      dest='input',\n      default='gs://dataflow-samples/shakespeare/kinglear.txt',\n      help='Input file to process.')\n  parser.add_argument(\n      '--output',\n      dest='output',\n      required=True,\n      help='Output file to write results to.')\n  known_args, pipeline_args = parser.parse_known_args(argv)\n\n  # We use the save_main_session option because one or more DoFn's in this\n  # workflow rely on global context (e.g., a module imported at module level).\n  pipeline_options = PipelineOptions(pipeline_args)\n  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session\n\n  # The pipeline will be run on exiting the with block.\n  with beam.Pipeline(options=pipeline_options) as p:\n\n    # Read the text file[pattern] into a PCollection.\n    lines = p | 'Read' >> ReadFromText(known_args.input)\n\n    counts = (\n        lines\n        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))\n        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))\n        | 'GroupAndSum' >> beam.CombinePerKey(sum))\n\n    # Format the counts into a PCollection of strings.\n    def format_result(word, count):\n      return '%s: %d' % (word, count)\n\n    output = counts | 'Format' >> beam.MapTuple(format_result)\n\n    # Write the output using a \"Write\" transform that has side effects.\n    # pylint: disable=expression-not-assigned\n    output | 'Write' >> WriteToText(known_args.output)\n\n\nif __name__ == '__main__':\n  logging.getLogger().setLevel(logging.INFO)\n  run()"
	javaCode          = "package org.apache.beam.examples;\n\n// beam-playground:\n//   name: MinimalWordCount\n//   description: An example that counts words in Shakespeare's works.\n//   multifile: false\n//   default_example: true\n//   context_line: 71\n//   categories:\n//     - Combiners\n//     - Filtering\n//     - IO\n//     - Core Transforms\n//     - Quickstart\n\nimport java.util.Arrays;\nimport org.apache.beam.sdk.Pipeline;\nimport org.apache.beam.sdk.io.TextIO;\nimport org.apache.beam.sdk.options.PipelineOptions;\nimport org.apache.beam.sdk.options.PipelineOptionsFactory;\nimport org.apache.beam.sdk.transforms.Count;\nimport org.apache.beam.sdk.transforms.Filter;\nimport org.apache.beam.sdk.transforms.FlatMapElements;\nimport org.apache.beam.sdk.transforms.MapElements;\nimport org.apache.beam.sdk.values.KV;\nimport org.apache.beam.sdk.values.TypeDescriptors;\n\n/**\n * An example that counts words in Shakespeare.\n *\n * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more\n * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or\n * argument processing, and focus on construction of the pipeline, which chains together the\n * application of core transforms.\n *\n * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the\n * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional\n * concepts.\n *\n * <p>Concepts:\n *\n * <pre>\n *   1. Reading data from text files\n *   2. Specifying 'inline' transforms\n *   3. Counting items in a PCollection\n *   4. Writing data to text files\n * </pre>\n *\n * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You\n * can see the results in the output files in your current working directory, with names like\n * \"wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate\n * file service.\n */\npublic class MinimalWordCount {\n\n  public static void main(String[] args) {\n\n    // Create a PipelineOptions object. This object lets us set various execution\n    // options for our pipeline, such as the runner you wish to use. This example\n    // will run with the DirectRunner by default, based on the class path configured\n    // in its dependencies.\n    PipelineOptions options = PipelineOptionsFactory.create();\n\n    // In order to run your pipeline, you need to make following runner specific changes:\n    //\n    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner\n    // or FlinkRunner.\n    // CHANGE 2/3: Specify runner-required options.\n    // For BlockingDataflowRunner, set project and temp location as follows:\n    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);\n    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);\n    //   dataflowOptions.setProject(\"SET_YOUR_PROJECT_ID_HERE\");\n    //   dataflowOptions.setTempLocation(\"gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY\");\n    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}\n    // for more details.\n    //   options.as(FlinkPipelineOptions.class)\n    //      .setRunner(FlinkRunner.class);\n\n    // Create the Pipeline object with the options we defined above\n    Pipeline p = Pipeline.create(options);\n\n    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set\n    // of input text files. TextIO.Read returns a PCollection where each element is one line from\n    // the input text (a set of Shakespeare's texts).\n\n    // This example reads from a public dataset containing the text of King Lear.\n    p.apply(TextIO.read().from(\"gs://apache-beam-samples/shakespeare/kinglear.txt\"))\n\n        // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.\n        // This transform splits the lines in PCollection<String>, where each element is an\n        // individual word in Shakespeare's collected texts.\n        .apply(\n            FlatMapElements.into(TypeDescriptors.strings())\n                .via((String line) -> Arrays.asList(line.split(\"[^\\\\p{L}]+\"))))\n        // We use a Filter transform to avoid empty word\n        .apply(Filter.by((String word) -> !word.isEmpty()))\n        // Concept #3: Apply the Count transform to our PCollection of individual words. The Count\n        // transform returns a new PCollection of key/value pairs, where each key represents a\n        // unique word in the text. The associated value is the occurrence count for that word.\n        .apply(Count.perElement())\n        // Apply a MapElements transform that formats our PCollection of word counts into a\n        // printable string, suitable for writing to an output file.\n        .apply(\n            MapElements.into(TypeDescriptors.strings())\n                .via(\n                    (KV<String, Long> wordCount) ->\n                        wordCount.getKey() + \": \" + wordCount.getValue()))\n        // Concept #4: Apply a write transform, TextIO.Write, at the end of the pipeline.\n        // TextIO.Write writes the contents of a PCollection (in this case, our PCollection of\n        // formatted strings) to a series of text files.\n        //\n        // By default, it will write to a set of files with names like wordcounts-00001-of-00005\n        .apply(TextIO.write().to(\"wordcounts\"));\n\n    p.run().waitUntilFinish();\n  }\n}"
	filePermission    = 0600
	fullPermission    = 0755
)

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
	defer teardown()
	m.Run()
}

func setup() error {
	err := os.Mkdir(sourceDir, fullPermission)
	if err != nil {
		return err
	}
	filePath := filepath.Join(sourceDir, fileName)
	err = os.WriteFile(filePath, []byte(fileContent), filePermission)
	javaFilePath := filepath.Join(sourceDir, javaFileName)
	err = os.WriteFile(javaFilePath, []byte(javaCode), filePermission)
	wordCountPythonPath := filepath.Join(sourceDir, pythonExampleName)
	err = os.WriteFile(wordCountPythonPath, []byte(wordCountPython), filePermission)
	return err
}

func teardown() error {
	return os.RemoveAll(sourceDir)
}

func TestReduceWhiteSpacesToSinge(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "Reduce white spaces to single", args: args{"--option1         option"}, want: "--option1 option"},
		{name: "Nothing to reduce", args: args{"--option1 option"}, want: "--option1 option"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReduceWhiteSpacesToSinge(tt.args.s); got != tt.want {
				t.Errorf("ReduceWhiteSpacesToSinge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadFile(t *testing.T) {
	type args struct {
		pipelineId uuid.UUID
		path       string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Read from existing file",
			args: args{
				pipelineId: uuid.New(),
				path:       filepath.Join(sourceDir, fileName),
			},
			want:    fileContent,
			wantErr: false,
		},
		{
			name: "Read from non-existent file",
			args: args{
				pipelineId: uuid.New(),
				path:       filepath.Join(sourceDir, "non-existent_file.txt"),
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadFile(tt.args.pipelineId, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}
