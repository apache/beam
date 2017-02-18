package dataflow

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

var (
	endpoint        = flag.String("api_root_url", "", "Dataflow endpoint (optional).")
	project         = flag.String("project", "", "Dataflow project.")
	jobName         = flag.String("job_name", "", "Dataflow job name (optional).")
	stagingLocation = flag.String("staging_location", os.ExpandEnv("gs://foo"), "GCS staging location.")

	// TODO(herohde) 2/14/2017: obtain a cross-compiled worker binary more elegantly. Reflect the top of the callstack?
	goProgram = flag.String("go_program", "", "Worker program to cross-compile (optional). Must be the same as the running program. If empty, binary is used.")
	goBinary  = flag.String("go_binary", "", "Cross-compiled worker binary (optional). If empty and program not specified, the present binary is used.")

	dryRun = flag.Bool("dry_run", false, "Dry run. Just print the job, but don't submit it.")
)

func Execute(ctx context.Context, p *beam.Pipeline) error {
	if *jobName == "" {
		*jobName = fmt.Sprintf("go-job-%v", time.Now().UnixNano())
	}

	edges, err := p.Build()
	if err != nil {
		return err
	}

	// (1) Upload Go binary to GCS.

	worker, err := buildBinary(*goProgram, *goBinary)
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
				Version: "0.1.0",
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
				WorkerHarnessContainerImage: "dataflow-dev.gcr.io/herohde/golang:latest",
				NumWorkers:                  1,
			}},
			TempStoragePrefix: *stagingLocation + "/tmp",
		},
		Steps: steps,
	}

	if *endpoint == "" {
		// TODO(herohde) 2/17/2017: until the new job type is in prod, we pretend to
		// be python.
		job.Environment.Version = newMsg(version{
			JobType: "PYTHON_BATCH",
			Major:   "5",
		})
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

	return local.Execute(ctx, p)
}

// stageWorker uploads the worker binary to GCS as a unique object.
func stageWorker(ctx context.Context, project, location, worker string) (string, error) {
	bucket, prefix, err := ParseObject(location)
	if err != nil {
		return "", fmt.Errorf("Invalid staging location %v: %v", location, err)
	}
	obj := path.Join(prefix, fmt.Sprintf("worker-%v", time.Now().UnixNano()))
	if *dryRun {
		full := fmt.Sprintf("gs://%v/%v", bucket, obj)
		log.Printf("Dry-run: not uploading binary %v", full)
		return full, nil
	}

	client, err := newStorageClient(ctx)
	if err != nil {
		return "", err
	}
	fd, err := os.Open(worker)
	if err != nil {
		return "", fmt.Errorf("Failed to open worker binary %s: %v", worker, err)
	}
	defer fd.Close()
	defer os.Remove(worker)

	return Upload(client, project, bucket, obj, fd)
}

// buildBinary creates (or finds) a local worker binary suitable to run on Dataflow.
func buildBinary(program, binary string) (string, error) {
	switch {
	case program != "":
		ret := filepath.Join(os.TempDir(), fmt.Sprintf("dataflow-go-%v", time.Now().UnixNano()))
		if *dryRun {
			log.Printf("Dry-run: not building binary %v", ret)
			return ret, nil
		}

		// Cross-compile given go program. Not awesome.
		real := []string{"go", "build", "-o", ret, program}

		cmd := exec.Command("/bin/bash", "-c", strings.Join(real, " "))
		cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
		if out, err := cmd.CombinedOutput(); err != nil {
			log.Print(string(out))
			return "", fmt.Errorf("Failed to cross-compile %v: %v", program, err)
		}
		return ret, nil

	case binary != "":
		return binary, nil

	default:
		return os.Args[0], nil
	}
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
	str, _ := json.MarshalIndent(job, "", "  ")
	log.Print(string(str))
}
