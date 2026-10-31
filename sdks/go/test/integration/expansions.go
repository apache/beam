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

package integration

import (
	"context"
	"fmt"
	"strconv"
	"time"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/jars"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ExpansionServices is a struct used for getting addresses and starting expansion services, based
// on the --expansion_jar and --expansion_addr flags in this package. The main reason to use this
// instead of accessing the flags directly is to let it handle jar startup and shutdown.
//
// # Usage
//
// Create an ExpansionServices object in TestMain with NewExpansionServices. Then use GetAddr for
// every expansion service needed for the test. Call Shutdown on it before finishing TestMain (or
// simply defer a call to it).
//
// ExpansionServices is not concurrency safe, and so a single instance should not be used within
// multiple individual tests, due to the possibility of those tests being run concurrently. It is
// recommended to only use ExpansionServices in TestMain to avoid this.
//
// Example:
//
//	flag.Parse()
//	beam.Init()
//	services := integration.NewExpansionServices()
//	defer func() { services.Shutdown() }()
//	addr, err := services.GetAddr("example")
//	if err != nil {
//	  panic(err)
//	}
//	expansionAddr = addr  // Save address to a package-level variable used by tests.
//	ptest.MainRet(m)
type ExpansionServices struct {
	addrs map[string]string
	jars  map[string]string
	procs []jars.Process
	// Callback for running jars, stored this way for testing purposes.
	run      func(time.Duration, string, ...string) (jars.Process, error)
	waitTime time.Duration // Time to sleep after running jar. Tests can adjust this.
	testMode bool          // Skip connectivity checks when in test mode
}

// NewExpansionServices creates and initializes an ExpansionServices instance.
func NewExpansionServices() *ExpansionServices {
	return &ExpansionServices{
		addrs:    GetExpansionAddrs(),
		jars:     GetExpansionJars(),
		procs:    make([]jars.Process, 0),
		run:      jars.Run,
		waitTime: 3 * time.Second,
		testMode: false,
	}
}

// GetAddr gets the address for the expansion service with the given label. The label corresponds to
// the labels used in the --expansion_jar and --expansion_addr flags. If an expansion service is
// provided as a jar, then that jar will be run to retrieve the address, and the jars are not
// guaranteed to be shut down unless Shutdown is called.
//
// Note: If this function starts a jar, it waits a few seconds for it to initialize. Do not use
// this function if the possibility of a few seconds of latency is not acceptable.
func (es *ExpansionServices) GetAddr(label string) (string, error) {
	// Always default to existing address before running a jar.
	if addr, ok := es.addrs[label]; ok {
		return addr, nil
	}
	jar, ok := es.jars[label]
	if !ok {
		err := fmt.Errorf("no --expansion_jar or --expansion_addr flag provided with label \"%s\"", label)
		return "", fmt.Errorf("expansion service labeled \"%s\" not found: %w", label, err)
	}

	// A different service may claim the port between selection and jar startup.
	const startupAttempts = 3
	for attempt := 0; ; attempt++ {
		port, err := ports.GetOpenTCP()
		if err != nil {
			return "", fmt.Errorf("cannot get open port for expansion service labeled \"%s\": %w", label, err)
		}
		portStr := strconv.Itoa(port)
		proc, err := es.run(*ExpansionTimeout, jar, portStr)
		if err != nil {
			return "", fmt.Errorf("cannot run jar for expansion service labeled \"%s\": %w", label, err)
		}
		addr := "localhost:" + portStr
		if es.testMode {
			time.Sleep(es.waitTime)
		} else {
			exited := make(chan error, 1)
			go func() { exited <- proc.Wait() }()
			if err := waitForExpansionService(addr, exited); err != nil {
				proc.Kill()
				// Retry only startup collisions, never expansion or pipeline RPCs.
				if status.Code(err) == codes.Unimplemented && attempt+1 < startupAttempts {
					continue
				}
				return "", fmt.Errorf("expansion service labeled \"%s\" failed to start at %s: %w", label, addr, err)
			}
		}
		es.procs = append(es.procs, proc)
		es.addrs[label] = addr
		return addr, nil
	}
}

func waitForExpansionService(addr string, exited <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := jobpb.NewExpansionServiceClient(conn)
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
		_, err := client.DiscoverSchemaTransform(probeCtx, &jobpb.DiscoverSchemaTransformRequest{}, grpc.WaitForReady(true))
		probeCancel()
		if status.Code(err) == codes.Unimplemented {
			// Older services only implement Expand. An empty request cannot expand
			// a transform, but an ExpansionResponse (including its error field)
			// proves that the service implements the expected RPC. Allow the
			// remaining startup deadline for first-request initialization.
			_, err = client.Expand(ctx, &jobpb.ExpansionRequest{}, grpc.WaitForReady(true))
		}
		if err == nil {
			select {
			case err := <-exited:
				return fmt.Errorf("jar exited before becoming ready: %v", err)
			default:
				return nil
			}
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return err
		}
		select {
		case err := <-exited:
			return fmt.Errorf("jar exited before becoming ready: %v", err)
		case <-ctx.Done():
			return fmt.Errorf("waiting for expansion API: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// Shutdown shuts down any jars started by the ExpansionServices struct and should get called if it
// was used at all.
func (es *ExpansionServices) Shutdown() {
	for _, p := range es.procs {
		p.Kill()
	}
	es.jars = nil
	es.addrs = nil
	es.procs = nil
}
