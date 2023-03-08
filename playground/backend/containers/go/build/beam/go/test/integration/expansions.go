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
	"fmt"
	"strconv"
	"time"

	"github.com/apache/beam/sdks/v2/go/test/integration/internal/jars"
	"github.com/apache/beam/sdks/v2/go/test/integration/internal/ports"
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
}

// NewExpansionServices creates and initializes an ExpansionServices instance.
func NewExpansionServices() *ExpansionServices {
	return &ExpansionServices{
		addrs:    GetExpansionAddrs(),
		jars:     GetExpansionJars(),
		procs:    make([]jars.Process, 0),
		run:      jars.Run,
		waitTime: 3 * time.Second,
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

	// Start jar on open port.
	port, err := ports.GetOpenTCP()
	if err != nil {
		return "", fmt.Errorf("cannot get open port for expansion service labeled \"%s\": %w", label, err)
	}
	portStr := strconv.Itoa(port)

	// Run jar and cache its info.
	proc, err := es.run(*ExpansionTimeout, jar, portStr)
	if err != nil {
		return "", fmt.Errorf("cannot run jar for expansion service labeled \"%s\": %w", label, err)
	}
	time.Sleep(es.waitTime) // Wait a bit for the jar to start.
	es.procs = append(es.procs, proc)
	addr := "localhost:" + portStr
	es.addrs[label] = addr
	return addr, nil
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
