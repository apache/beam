package preparers

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/validators"
)

const (
	correctGoCode   = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\")\n\n\n}\n"
	correctGoFile   = "correct.go"
	incorrectGoCode = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\"\n\n}\n"
	incorrectGoFile = "incorrect.go"
	pyCode          = "import logging as l\n\nif __name__ == \"__main__\":\n    logging.info(\"INFO\")\n"
	correctPyFile   = "original.py"
	incorrectPyFile = "someFile.py"
	pyGraphCode     = "#\n# Licensed to the Apache Software Foundation (ASF) under one or more\n# contributor license agreements.  See the NOTICE file distributed with\n# this work for additional information regarding copyright ownership.\n# The ASF licenses this file to You under the Apache License, Version 2.0\n# (the \"License\"); you may not use this file except in compliance with\n# the License.  You may obtain a copy of the License at\n#\n#    http://www.apache.org/licenses/LICENSE-2.0\n#\n# Unless required by applicable law or agreed to in writing, software\n# distributed under the License is distributed on an \"AS IS\" BASIS,\n# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n# See the License for the specific language governing permissions and\n# limitations under the License.\n#\n\n\"\"\"An example that verifies the counts and includes best practices.\n\nOn top of the basic concepts in the wordcount example, this workflow introduces\nlogging to Cloud Logging, and using assertions in a Dataflow pipeline.\n\nTo execute this pipeline locally, specify a local output file or output prefix\non GCS::\n\n  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]\n\nTo execute this pipeline using the Google Cloud Dataflow service, specify\npipeline configuration::\n\n  --project YOUR_PROJECT_ID\n  --staging_location gs://YOUR_STAGING_DIRECTORY\n  --temp_location gs://YOUR_TEMP_DIRECTORY\n  --region GCE_REGION\n  --job_name YOUR_JOB_NAME\n  --runner DataflowRunner\n\nand an output prefix on GCS::\n\n  --output gs://YOUR_OUTPUT_PREFIX\n\"\"\"\n\n# pytype: skip-file\n\nimport argparse\nimport logging\nimport re\n\nimport apache_beam as beam\nfrom apache_beam.io import ReadFromText\nfrom apache_beam.io import WriteToText\nfrom apache_beam.metrics import Metrics\nfrom apache_beam.options.pipeline_options import PipelineOptions\nfrom apache_beam.options.pipeline_options import SetupOptions\nfrom apache_beam.testing.util import assert_that\nfrom apache_beam.testing.util import equal_to\n\n\nclass FilterTextFn(beam.DoFn):\n  \"\"\"A DoFn that filters for a specific key based on a regular expression.\"\"\"\n  def __init__(self, pattern):\n    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.\n    # super().__init__()\n    beam.DoFn.__init__(self)\n    self.pattern = pattern\n    # A custom metric can track values in your pipeline as it runs. Those\n    # values will be available in the monitoring system of the runner used\n    # to run the pipeline. These metrics below track the number of\n    # matched and unmatched words.\n    self.matched_words = Metrics.counter(self.__class__, 'matched_words')\n    self.umatched_words = Metrics.counter(self.__class__, 'umatched_words')\n\n  def process(self, element):\n    word, _ = element\n    if re.match(self.pattern, word):\n      # Log at INFO level each element we match. When executing this pipeline\n      # using the Dataflow service, these log lines will appear in the Cloud\n      # Logging UI.\n      logging.info('Matched %s', word)\n      self.matched_words.inc()\n      yield element\n    else:\n      # Log at the \"DEBUG\" level each element that is not matched. Different log\n      # levels can be used to control the verbosity of logging providing an\n      # effective mechanism to filter less important information.\n      # Note currently only \"INFO\" and higher level logs are emitted to the\n      # Cloud Logger. This log message will not be visible in the Cloud Logger.\n      logging.debug('Did not match %s', word)\n      self.umatched_words.inc()\n\n\nclass CountWords(beam.PTransform):\n  \"\"\"A transform to count the occurrences of each word.\n\n  A PTransform that converts a PCollection containing lines of text into a\n  PCollection of (word, count) tuples.\n  \"\"\"\n  def expand(self, pcoll):\n    def count_ones(word_ones):\n      (word, ones) = word_ones\n      return (word, sum(ones))\n\n    return (\n        pcoll\n        | 'split' >> (\n            beam.FlatMap(\n                lambda x: re.findall(r'[A-Za-z\\']+', x)).with_output_types(str))\n        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))\n        | 'group' >> beam.GroupByKey()\n        | 'count' >> beam.Map(count_ones))\n\n\ndef run(argv=None, save_main_session=True):\n  \"\"\"Runs the debugging wordcount pipeline.\"\"\"\n\n  parser = argparse.ArgumentParser()\n  parser.add_argument(\n      '--input',\n      dest='input',\n      default='gs://dataflow-samples/shakespeare/kinglear.txt',\n      help='Input file to process.')\n  parser.add_argument(\n      '--output',\n      dest='output',\n      required=True,\n      help='Output file to write results to.')\n  known_args, pipeline_args = parser.parse_known_args(argv)\n  # We use the save_main_session option because one or more DoFn's in this\n  # workflow rely on global context (e.g., a module imported at module level).\n  pipeline_options = PipelineOptions(pipeline_args)\n  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session\n  with beam.Pipeline(options=pipeline_options) as p:\n\n    # Read the text file[pattern] into a PCollection, count the occurrences of\n    # each word and filter by a list of words.\n    filtered_words = (\n        p | 'read' >> ReadFromText(known_args.input)\n        | CountWords()\n        | 'FilterText' >> beam.ParDo(FilterTextFn('Flourish|stomach')))\n\n    # assert_that is a convenient PTransform that checks a PCollection has an\n    # expected value. Asserts are best used in unit tests with small data sets\n    # but is demonstrated here as a teaching tool.\n    #\n    # Note assert_that does not provide any output and that successful\n    # completion of the Pipeline implies that the expectations were  met. Learn\n    # more at https://cloud.google.com/dataflow/pipelines/testing-your-pipeline\n    # on how to best test your pipeline.\n    assert_that(filtered_words, equal_to([('Flourish', 3), ('stomach', 1)]))\n\n    # Format the counts into a PCollection of strings and write the output using\n    # a \"Write\" transform that has side effects.\n    # pylint: disable=unused-variable\n    def format_result(word_count):\n      (word, count) = word_count\n      return '%s: %s' % (word, count)\n\n    output = (\n        filtered_words\n        | 'format' >> beam.Map(format_result)\n        | 'write' >> WriteToText(known_args.output))\n\n\nif __name__ == '__main__':\n  # Cloud Logging would contain only logging.INFO and higher level logs logged\n  # by the root logger. All log statements emitted by the root logger will be\n  # visible in the Cloud Logging UI. Learn more at\n  # https://cloud.google.com/logging about the Cloud Logging UI.\n  #\n  # You can set the default logging level to a different level when running\n  # locally.\n  logging.getLogger().setLevel(logging.INFO)\n  run()\n"
	pyGraphFile     = "graphFile.py"
)

func TestMain(m *testing.M) {
	err := setupPreparedFiles()
	if err != nil {
		logger.Fatal(err)
	}
	defer teardown()
	m.Run()
}

// setupPreparedFiles creates go files used for tests
func setupPreparedFiles() error {
	err := createFile(correctGoFile, correctGoCode)
	if err != nil {
		return err
	}
	err = createFile(incorrectGoFile, incorrectGoCode)
	if err != nil {
		return err
	}
	err = createFile(correctPyFile, pyCode)
	if err != nil {
		return err
	}
	err = createFile(pyGraphFile, pyGraphCode)
	if err != nil {
		return err
	}
	return nil
}

// createFile create file with fileName and write text to it
func createFile(fileName, text string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(text))
	if err != nil {
		return err
	}
	return nil
}

func teardown() {
	files := []string{correctGoFile, incorrectGoFile, correctPyFile, incorrectPyFile, pyGraphFile,
		fmt.Sprintf("%s_%s", "tmp", correctPyFile), fmt.Sprintf("%s_%s", "tmp", pyGraphFile)}
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			logger.Fatal(err)
		}
	}
}

func TestGetPreparers(t *testing.T) {
	filepath := ""
	prepareParams := make(map[string]string)
	validationResults := sync.Map{}
	validationResults.Store(validators.UnitTestValidatorName, false)
	validationResults.Store(validators.KatasValidatorName, false)

	type args struct {
		sdk           pb.Sdk
		filepath      string
		prepareParams map[string]string
		valResults    *sync.Map
	}
	tests := []struct {
		name    string
		args    args
		want    *[]Preparer
		wantErr bool
	}{
		{
			// Test case with calling GetPreparers method with Java sdk.
			// As a result, want to receive 3 preparers
			name: "Get Java preparers",
			args: args{
				sdk:           pb.Sdk_SDK_JAVA,
				filepath:      filepath,
				prepareParams: prepareParams,
				valResults:    &validationResults,
			},
			want: NewPreparersBuilder(filepath, prepareParams).
				JavaPreparers().
				WithPublicClassRemover().
				WithPackageChanger().
				WithGraphHandler().
				Build().
				GetPreparers(),
			wantErr: false,
		},
		{
			// Test case with calling GetPreparers method with Python sdk.
			// As a result, want to receive 2 preparers
			name: "Get Python Preparers",
			args: args{
				sdk:           pb.Sdk_SDK_PYTHON,
				filepath:      filepath,
				prepareParams: prepareParams,
				valResults:    &validationResults,
			},
			want: NewPreparersBuilder(filepath, prepareParams).
				PythonPreparers().
				WithLogHandler().
				WithGraphHandler().
				Build().
				GetPreparers(),
			wantErr: false,
		},
		{
			// Test case with calling GetPreparers method with Go sdk.
			// As a result, want to receive 1 preparer
			name: "Get Go preparer",
			args: args{
				sdk:           pb.Sdk_SDK_GO,
				filepath:      filepath,
				prepareParams: prepareParams,
				valResults:    &validationResults,
			},
			want: NewPreparersBuilder(filepath, prepareParams).
				GoPreparers().
				WithCodeFormatter().
				Build().
				GetPreparers(),
			wantErr: false,
		},
		{
			// Test case with calling GetPreparers method with Scio sdk.
			// As a result, want to receive 3 preparers
			name: "Get Scio preparers",
			args: args{
				sdk:           pb.Sdk_SDK_SCIO,
				filepath:      filepath,
				prepareParams: prepareParams,
				valResults:    &validationResults,
			},
			want: NewPreparersBuilder(filepath, prepareParams).
				ScioPreparers().
				WithPackageRemover().
				WithImportRemover().
				WithFileNameChanger().
				Build().
				GetPreparers(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetPreparers(tt.args.sdk, tt.args.filepath, tt.args.valResults, tt.args.prepareParams)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPreparers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(len(*got), len(*tt.want)) {
				t.Errorf("GetPreparers() got = %v, want %v", len(*got), len(*tt.want))
			}
		})
	}
}
