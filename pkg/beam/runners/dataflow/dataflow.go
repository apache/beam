package dataflow

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/util/storagex"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
)

// TODO(herohde) 5/16/2017: the Dataflow flags should match the other SDKs.

var (
	endpoint        = flag.String("api_root_url", "", "Dataflow endpoint (optional).")
	project         = flag.String("project", "", "Dataflow project.")
	jobName         = flag.String("job_name", "", "Dataflow job name (optional).")
	stagingLocation = flag.String("staging_location", os.ExpandEnv("gs://foo"), "GCS staging location.")
	image           = flag.String("worker_harness_container_image", "", "Worker harness container image.")
	numWorkers      = flag.Int64("num_workers", 0, "Number of workers (optional).")
	experiments     = flag.String("experiments", "", "Comma-separated list of experiments (optional).")

	dryRun         = flag.Bool("dry_run", false, "Dry run. Just print the job, but don't submit it.")
	block          = flag.Bool("block", true, "Wait for job to terminate.")
	teardownPolicy = flag.String("teardown_policy", "", "Job teardown policy (internal only).")
)

// Execute runs the given pipeline on Google Cloud Dataflow. It uses the
// default application credentials to submit the job.
func Execute(ctx context.Context, p *beam.Pipeline) error {
	if *jobName == "" {
		*jobName = fmt.Sprintf("go-%v-%v", username(), time.Now().UnixNano())
	}

	edges, _, err := p.Build()
	if err != nil {
		return err
	}

	// (1) Upload Go binary to GCS.

	worker, err := buildLocalBinary()
	if err != nil {
		return err
	}
	binary, err := stageWorker(ctx, *project, *stagingLocation, worker)
	if err != nil {
		return err
	}

	// (2) Translate pipeline to v1b3 speak.

	steps, err := translate(edges)
	if err != nil {
		return err
	}

	job := &df.Job{
		ProjectId: *project,
		Name:      *jobName,
		Type:      "JOB_TYPE_BATCH",
		Environment: &df.Environment{
			UserAgent: newMsg(userAgent{
				Name:    "Apache Beam SDK for Go",
				Version: "0.3.0",
			}),
			Version: newMsg(version{
				JobType: "FNAPI_BATCH",
				Major:   "1",
			}),
			SdkPipelineOptions: newMsg(pipelineOptions{
				DisplayData: findPipelineFlags(),
			}),
			WorkerPools: []*df.WorkerPool{{
				Kind: "harness",
				Packages: []*df.Package{{
					Location: binary,
					Name:     "worker",
				}},
				WorkerHarnessContainerImage: *image,
				NumWorkers:                  1,
			}},
			TempStoragePrefix: *stagingLocation + "/tmp",
		},
		Steps: steps,
	}

	if *numWorkers != 0 {
		job.Environment.WorkerPools[0].NumWorkers = *numWorkers
	}
	if *teardownPolicy != "" {
		job.Environment.WorkerPools[0].TeardownPolicy = *teardownPolicy
	}
	if *experiments != "" {
		job.Environment.Experiments = strings.Split(*experiments, ",")
	}
	printJob(job)

	if *dryRun {
		log.Print("Dry-run: not submitting job!")
		return nil
	}

	// (3) Submit job.

	client, err := newClient(ctx, *endpoint)
	if err != nil {
		return err
	}
	upd, err := client.Projects.Jobs.Create(*project, job).Do()
	if err != nil {
		return err
	}

	log.Printf("Submitted job: %v", upd.Id)
	printJob(upd)
	if *endpoint == "" {
		log.Printf("Console: https://console.cloud.google.com/dataflow/job/%v?project=%v", upd.Id, *project)
	}
	log.Printf("Logs: https://console.cloud.google.com/logs/viewer?project=%v&resource=dataflow_step%%2Fjob_id%%2F%v", *project, upd.Id)

	if !*block {
		return nil
	}

	time.Sleep(1 * time.Minute)
	for {
		j, err := client.Projects.Jobs.Get(*project, upd.Id).Do()
		if err != nil {
			return fmt.Errorf("failed to get job: %v", err)
		}

		switch j.CurrentState {
		case "JOB_STATE_DONE":
			log.Print("Job succeeded!")
			return nil

		case "JOB_STATE_FAILED":
			return fmt.Errorf("job %s failed", upd.Id)

		case "JOB_STATE_RUNNING":
			log.Print("Job still running ...")

		default:
			log.Printf("Job state: %v ...", j.CurrentState)
		}

		time.Sleep(30 * time.Second)
	}
}

// stageWorker uploads the worker binary to GCS as a unique object.
func stageWorker(ctx context.Context, project, location, worker string) (string, error) {
	bucket, prefix, err := storagex.ParseObject(location)
	if err != nil {
		return "", fmt.Errorf("invalid staging location %v: %v", location, err)
	}
	obj := path.Join(prefix, fmt.Sprintf("worker-%v", time.Now().UnixNano()))
	if *dryRun {
		full := fmt.Sprintf("gs://%v/%v", bucket, obj)
		log.Printf("Dry-run: not uploading binary %v", full)
		return full, nil
	}

	client, err := storagex.NewClient(ctx)
	if err != nil {
		return "", err
	}
	fd, err := os.Open(worker)
	if err != nil {
		return "", fmt.Errorf("failed to open worker binary %s: %v", worker, err)
	}
	defer fd.Close()
	defer os.Remove(worker)

	return storagex.Upload(client, project, bucket, obj, fd)
}

// buildLocalBinary creates a local worker binary suitable to run on Dataflow. It finds the filename
// by examining the call stack. We want the user entry (*), for example:
//
//   /Users/herohde/go/src/github.com/apache/beam/sdks/go/pkg/beam/runners/beamexec/main.go (skip: 2)
// * /Users/herohde/go/src/github.com/apache/beam/sdks/go/examples/wordcount/wordcount.go (skip: 3)
//   /usr/local/go/src/runtime/proc.go (skip: 4)
//   /usr/local/go/src/runtime/asm_amd64.s (skip: 5)
func buildLocalBinary() (string, error) {
	ret := filepath.Join(os.TempDir(), fmt.Sprintf("dataflow-go-%v", time.Now().UnixNano()))
	if *dryRun {
		log.Printf("Dry-run: not building binary %v", ret)
		return ret, nil
	}

	program := ""
	for i := 3; ; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok || strings.HasSuffix(file, "runtime/proc.go") {
			break
		}
		program = file
	}
	if program == "" {
		return "", fmt.Errorf("could not detect user main")
	}

	log.Printf("Cross-compiling %v as %v", program, ret)

	// Cross-compile given go program. Not awesome.
	real := []string{"go", "build", "-o", ret, program}

	cmd := exec.Command("/bin/bash", "-c", strings.Join(real, " "))
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Print(string(out))
		return "", fmt.Errorf("failed to cross-compile %v: %v", program, err)
	}
	return ret, nil
}

func username() string {
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return "anon"
}

func findPipelineFlags() []*displayData {
	var ret []*displayData

	// TODO(herohde) 2/15/2017: decide if we want all set flags.
	flag.Visit(func(f *flag.Flag) {
		ret = append(ret, newDisplayData(f.Name, "", "flag", f.Value.(flag.Getter).Get()))
	})

	return ret
}

// newClient creates a new dataflow client with default application credentials
// and CloudPlatformScope. The BasePath is optionally overridden.
func newClient(ctx context.Context, basePath string) (*df.Service, error) {
	cl, err := google.DefaultClient(ctx, df.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	client, err := df.New(cl)
	if err != nil {
		return nil, err
	}
	if basePath != "" {
		log.Printf("Dataflow base path override: %s", basePath)
		client.BasePath = basePath
	}
	return client, nil
}

func printJob(job *df.Job) {
	str, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		log.Printf("Failed to print job %v: %v", job.Id, err)
	}
	log.Print(string(str))
}
