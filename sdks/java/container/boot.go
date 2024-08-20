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

// boot is the boot code for the Java SDK harness container. It is responsible
// for retrieving staged files and invoking the JVM correctly.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/syscallx"
)

var (
	// Contract: https://s.apache.org/beam-fn-api-container-contract.

	id                = flag.String("id", "", "Local identifier (required).")
	loggingEndpoint   = flag.String("logging_endpoint", "", "Logging endpoint (required).")
	artifactEndpoint  = flag.String("artifact_endpoint", "", "Artifact endpoint (required).")
	provisionEndpoint = flag.String("provision_endpoint", "", "Provision endpoint (required).")
	controlEndpoint   = flag.String("control_endpoint", "", "Control endpoint (required).")
	semiPersistDir    = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional).")
)

const (
	disableJammAgentOption              = "disable_jamm_agent"
	enableGoogleCloudProfilerOption     = "enable_google_cloud_profiler"
	enableGoogleCloudHeapSamplingOption = "enable_google_cloud_heap_sampling"
	googleCloudProfilerAgentBaseArgs    = "-agentpath:/opt/google_cloud_profiler/profiler_java_agent.so=-logtostderr,-cprof_service=%s,-cprof_service_version=%s"
	googleCloudProfilerAgentHeapArgs    = googleCloudProfilerAgentBaseArgs + ",-cprof_enable_heap_sampling,-cprof_heap_sampling_interval=2097152"
	jammAgentArgs                       = "-javaagent:/opt/apache/beam/jars/jamm.jar"
)

func main() {
	flag.Parse()
	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	info, err := tools.ProvisionInfo(ctx, *provisionEndpoint)
	if err != nil {
		log.Fatalf("Failed to obtain provisioning information: %v", err)
	}
	log.Printf("Provision info:\n%v", info)

	// TODO(BEAM-8201): Simplify once flags are no longer used.
	if info.GetLoggingEndpoint().GetUrl() != "" {
		*loggingEndpoint = info.GetLoggingEndpoint().GetUrl()
	}
	if info.GetArtifactEndpoint().GetUrl() != "" {
		*artifactEndpoint = info.GetArtifactEndpoint().GetUrl()
	}
	if info.GetControlEndpoint().GetUrl() != "" {
		*controlEndpoint = info.GetControlEndpoint().GetUrl()
	}

	if *loggingEndpoint == "" {
		log.Fatal("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		log.Fatal("No artifact endpoint provided.")
	}
	if *controlEndpoint == "" {
		log.Fatal("No control endpoint provided.")
	}
	logger := &tools.Logger{Endpoint: *loggingEndpoint}

	logger.Printf(ctx, "Initializing java harness: %v", strings.Join(os.Args, " "))

	// (1) Obtain the pipeline options
	options, err := tools.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		logger.Fatalf(ctx, "Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve the staged user jars. We ignore any disk limit,
	// because the staged jars are mandatory.

	// Using the SDK Harness ID in the artifact destination path to make sure that dependencies used by multiple
	// SDK Harnesses in the same VM do not conflict. This is needed since some runners (for example, Dataflow)
	// may share the artifact staging directory across multiple SDK Harnesses
	// TODO(https://github.com/apache/beam/issues/20009): consider removing the SDK Harness ID from the staging path after Dataflow can properly
	// seperate out dependencies per environment.
	dir := filepath.Join(*semiPersistDir, *id, "staged")

	artifacts, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetDependencies(), info.GetRetrievalToken(), dir)
	if err != nil {
		logger.Fatalf(ctx, "Failed to retrieve staged files: %v", err)
	}

	// (3) Invoke the Java harness, preserving artifact ordering in classpath.

	os.Setenv("HARNESS_ID", *id)
	if err := tools.MakePipelineOptionsFileAndEnvVar(options); err != nil {
		logger.Fatalf(ctx, "Failed to load pipeline options to worker: %v", err)
	}
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", (&pipepb.ApiServiceDescriptor{Url: *loggingEndpoint}).String())
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", (&pipepb.ApiServiceDescriptor{Url: *controlEndpoint}).String())
	os.Setenv("RUNNER_CAPABILITIES", strings.Join(info.GetRunnerCapabilities(), " "))

	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_API_SERVICE_DESCRIPTOR", info.GetStatusEndpoint().String())
	}

	const jarsDir = "/opt/apache/beam/jars"
	cp := []string{
		filepath.Join(jarsDir, "slf4j-api.jar"),
		filepath.Join(jarsDir, "slf4j-jdk14.jar"),
		filepath.Join(jarsDir, "jcl-over-slf4j.jar"),
		filepath.Join(jarsDir, "log4j-over-slf4j.jar"),
		filepath.Join(jarsDir, "log4j-to-slf4j.jar"),
		filepath.Join(jarsDir, "beam-sdks-java-harness.jar"),
	}

	var hasWorkerExperiment = strings.Contains(options, "use_staged_dataflow_worker_jar")
	for _, a := range artifacts {
		name, _ := artifact.MustExtractFilePayload(a)
		if hasWorkerExperiment {
			if strings.HasPrefix(name, "beam-runners-google-cloud-dataflow-java-fn-api-worker") {
				continue
			}
			if name == "dataflow-worker.jar" {
				continue
			}
		}
		cp = append(cp, filepath.Join(dir, filepath.FromSlash(name)))
	}

	var lim uint64
	if strings.Contains(options, "set_recommended_max_xmx") {
		lim = 32 << 30
	} else {
		size, err := syscallx.PhysicalMemorySize()
		if err != nil {
			size = 0
		}
		lim = HeapSizeLimit(size)
	}

	args := []string{
		"-Xmx" + strconv.FormatUint(lim, 10),
		// ParallelGC the most adequate for high throughput and lower CPU utilization
		// It is the default GC in Java 8, but not on newer versions
		"-XX:+UseParallelGC",
		"-XX:+AlwaysActAsServerClassMachine",
		"-XX:-OmitStackTraceInFastThrow",
	}

	enableGoogleCloudProfiler := strings.Contains(options, enableGoogleCloudProfilerOption)
	enableGoogleCloudHeapSampling := strings.Contains(options, enableGoogleCloudHeapSamplingOption)
	if enableGoogleCloudProfiler {
		if metadata := info.GetMetadata(); metadata != nil {
			if jobName, nameExists := metadata["job_name"]; nameExists {
				if jobId, idExists := metadata["job_id"]; idExists {
					if enableGoogleCloudHeapSampling {
						args = append(args, fmt.Sprintf(googleCloudProfilerAgentHeapArgs, jobName, jobId))
					} else {
						args = append(args, fmt.Sprintf(googleCloudProfilerAgentBaseArgs, jobName, jobId))
					}
					logger.Printf(ctx, "Turning on Cloud Profiling. Profile heap: %t", enableGoogleCloudHeapSampling)
				} else {
					logger.Printf(ctx, "Required job_id missing from metadata, profiling will not be enabled without it.")
				}
			} else {
				logger.Printf(ctx, "Required job_name missing from metadata, profiling will not be enabled without it.")
			}
		} else {
			logger.Printf(ctx, "enable_google_cloud_profiler is set to true, but no metadata is received from provision server, profiling will not be enabled.")
		}
	}

	disableJammAgent := strings.Contains(options, disableJammAgentOption)
	if disableJammAgent {
		logger.Printf(ctx, "Disabling Jamm agent. Measuring object size will be inaccurate.")
	} else {
		args = append(args, jammAgentArgs)
	}
	// Apply meta options
	const metaDir = "/opt/apache/beam/options"

	// Note: Error is unchecked, so parsing errors won't abort container.
	// TODO: verify if it's intentional or not.
	metaOptions, _ := LoadMetaOptions(ctx, logger, metaDir)

	javaOptions := BuildOptions(ctx, logger, metaOptions)
	// (1) Add custom jvm arguments: "-server -Xmx1324 -XXfoo .."
	args = append(args, javaOptions.JavaArguments...)

	// (2) Add classpath: "-cp foo.jar:bar.jar:.."
	if len(javaOptions.Classpath) > 0 {
		cp = append(cp, javaOptions.Classpath...)
	}
	pathingjar, err := makePathingJar(cp)
	if err != nil {
		logger.Fatalf(ctx, "makePathingJar failed: %v", err)
	}
	args = append(args, "-cp")
	args = append(args, pathingjar)

	// (3) Add (sorted) properties: "-Dbar=baz -Dfoo=bar .."
	var properties []string
	for key, value := range javaOptions.Properties {
		properties = append(properties, fmt.Sprintf("-D%s=%s", key, value))
	}
	sort.Strings(properties)
	args = append(args, properties...)

	// Open modules specified in pipeline options
	if pipelineOptions, ok := info.GetPipelineOptions().GetFields()["options"]; ok {
		if modules, ok := pipelineOptions.GetStructValue().GetFields()["jdkAddOpenModules"]; ok {
			for _, module := range modules.GetListValue().GetValues() {
				args = append(args, "--add-opens="+module.GetStringValue())
			}
		}
	}
	// Automatically open modules for Java 11+
	openModuleAgentJar := "/opt/apache/beam/jars/open-module-agent.jar"
	if _, err := os.Stat(openModuleAgentJar); err == nil {
		args = append(args, "-javaagent:"+openModuleAgentJar)
	}
	args = append(args, "org.apache.beam.fn.harness.FnHarness")
	logger.Printf(ctx, "Executing: java %v", strings.Join(args, " "))

	logger.Fatalf(ctx, "Java exited: %v", execx.Execute("java", args...))
}

// heapSizeLimit returns 80% of the runner limit, if provided. If not provided,
// it returns max(70% size, size - 32GB). Set size=0 if the physical memory on
// the machine was undetermined, then it returns 1GB. This is an imperfect
// heuristic. It aims to ensure there is memory for non-heap use and other
// overhead, while also not underutilizing the machine.
// if set_recommended_max_xmx experiment is enabled, sets xmx to 32G. Under 32G
// JVM enables CompressedOops. CompressedOops utilizes memory more efficiently,
// and has positive impact on GC performance and cache hit rate.
func HeapSizeLimit(size uint64) uint64 {
	if size == 0 {
		return 1 << 30
	}
	lim := (size * 70) / 100
	if size-lim < 32<<30 {
		return lim
	}
	return size - (32 << 30)
}

// Options represents java VM invocation options in a simple,
// semi-structured way.
type Options struct {
	JavaArguments []string          `json:"java_arguments,omitempty"`
	Properties    map[string]string `json:"properties,omitempty"`
	Classpath     []string          `json:"classpath,omitempty"`
}

// MetaOption represents a jvm environment transformation or setup
// that the launcher employs. The aim is to keep the service-side and
// user-side required configuration simple and minimal, yet allow
// numerous execution tweaks. Most tweaks are enabled by default and
// require no input. Some setups, such as Cloud Debugging, are opt-in.
//
// Meta-options are usually included with the image and use supporting
// files, usually jars. A few are intrinsic because they are require
// additional input or complex computations, such as Cloud Debugging
// and Cloud Profiling. Meta-options can be enabled or disabled by
// name. For the most part, the meta-option names are not guaranteed
// to be backwards compatible or stable. They are rather knobs that
// can be tuned if some well-intended transformation cause trouble for
// a customer. For tweaks, the expectation is that the default is
// almost always correct.
//
// Meta-options are simple additive manipulations applied in priority
// order (applied low to high) to allow jvm customization by adding
// files, notably enabling customization by later docker layers. The
// override semantics is prepend for lists and simple overwrite
// otherwise. A common use case is adding a jar to the beginning of
// the classpath, such as the shuffle or windmill jni jar, or adding
// an agent.
type MetaOption struct {
	Name        string  `json:"name,omitempty"`
	Description string  `json:"description,omitempty"`
	Enabled     bool    `json:"enabled,omitempty"`
	Priority    int     `json:"priority,omitempty"`
	Options     Options `json:"options"`
}

// byPriority sorts MetaOptions by priority, highest first.
type byPriority []*MetaOption

func (f byPriority) Len() int           { return len(f) }
func (f byPriority) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f byPriority) Less(i, j int) bool { return f[i].Priority > f[j].Priority }

// LoadMetaOptions scans the directory tree for meta-option metadata
// files and loads them. Any regular file named "option-XX.json" is
// strictly assumed to be a meta-option file. This strictness allows
// us to fail hard if such a file cannot be parsed.
//
// Loading meta-options from disk allows extra files and their
// configuration be kept together and defined externally.
func LoadMetaOptions(ctx context.Context, logger *tools.Logger, dir string) ([]*MetaOption, error) {
	var meta []*MetaOption

	worker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if !strings.HasPrefix(info.Name(), "option-") {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		var option MetaOption
		if err := json.Unmarshal(content, &option); err != nil {
			return fmt.Errorf("failed to parse %s: %v", path, err)
		}

		logger.Printf(ctx, "Loaded meta-option '%s'", option.Name)

		meta = append(meta, &option)
		return nil
	}

	if err := filepath.Walk(dir, worker); err != nil {
		return nil, err
	}
	return meta, nil
}

func BuildOptions(ctx context.Context, logger *tools.Logger, metaOptions []*MetaOption) *Options {
	options := &Options{Properties: make(map[string]string)}

	sort.Sort(byPriority(metaOptions))
	for _, meta := range metaOptions {
		if !meta.Enabled {
			continue
		}

		// Rightmost takes precedence
		options.JavaArguments = append(meta.Options.JavaArguments, options.JavaArguments...)

		for key, value := range meta.Options.Properties {
			_, exists := options.Properties[key]
			if !exists {
				options.Properties[key] = value
			} else {
				logger.Warnf(ctx, "Warning: %s property -D%s=%s was redefined", meta.Name, key, value)
			}
		}

		options.Classpath = append(options.Classpath, meta.Options.Classpath...)
	}
	return options
}
