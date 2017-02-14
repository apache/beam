package dataflow

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"golang.org/x/oauth2/google"
	df "google.golang.org/api/dataflow/v1b3"
	"log"
	"os"
)

var (
	endpoint        = flag.String("endpoint", "", "Dataflow endpoint (optional).")
	project         = flag.String("project", "", "Dataflow project.")
	jobName         = flag.String("job_name", os.ExpandEnv("the-go-job-$USER"), "Dataflow job name (optional).")
	stagingLocation = flag.String("staging_location", os.ExpandEnv("gs://foo"), "GCS staging location.")
)

func Execute(ctx context.Context, p *beam.Pipeline) error {
	edges, err := p.Build()
	if err != nil {
		return err
	}

	// (1) TODO(herohde) 2/10/2017: Upload Go binary to GCS.

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
				// TODO: generic SDK setup
				JobType: "PYTHON_BATCH",
				Major:   "5",
			}),
			WorkerPools: []*df.WorkerPool{{
				Kind:     "harness",
				Packages: []*df.Package{
				// TODO: Go binary as packages
				},
				WorkerHarnessContainerImage: "",
				NumWorkers:                  1,
			}},
			TempStoragePrefix: *stagingLocation + "/tmp",
		},
		Steps: steps,
	}

	printJob(job)

	// (2) Submit job.

	client, err := NewClient(ctx, *endpoint)
	if err != nil {
		return err
	}
	upd, err := client.Projects.Jobs.Create(*project, job).Do()
	if err != nil {
		return err
	}

	log.Printf("Submitted: %v", upd.Id)
	printJob(upd)

	return local.Execute(ctx, p)
}

// NewClient creates a new dataflow client with default application credentials
// and CloudPlatformScope. The BasePath is optionally overridden.
func NewClient(ctx context.Context, basePath string) (*df.Service, error) {
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
