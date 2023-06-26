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

package datastore

import (
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

const (
	emulatorHost    = "127.0.0.1"
	portRangeStart  = 10000
	portRangeEnd    = 20000
	maxAttempts     = 3
	startupDuration = 500 * time.Millisecond
	pauseDuration   = 100 * time.Millisecond
	waitDuration    = 10 * time.Second
)

type EmulatedDatastore struct {
	*Datastore
	emulator *emulator
}

type emulator struct {
	Host string
	Port int
	cmd  *exec.Cmd
}

func (ed *EmulatedDatastore) Close() error {
	clientCloseErr := ed.Datastore.Client.Close()
	// Stop emulator no matter if client was closed successfully or not.
	// We don't want to keep emulator processes running after test is ended.
	emulatorStopErr := ed.emulator.Stop()
	if clientCloseErr != nil {
		return clientCloseErr
	}
	if emulatorStopErr != nil {
		return emulatorStopErr
	}
	return nil
}

func NewEmulated(ctx context.Context) (*EmulatedDatastore, error) {
	emulator, err := startEmulator()
	if err != nil {
		return nil, err
	}

	if _, ok := os.LookupEnv("GOOGLE_CLOUD_PROJECT"); ok {
		err := os.Unsetenv("GOOGLE_CLOUD_PROJECT")
		if err != nil {
			panic(err)
		}
	}

	if err := os.Setenv(constants.EmulatorHostKey, emulator.GetEndpoint()); err != nil {
		panic(err)
	}

	datastoreDb, err := New(ctx, mapper.NewPrecompiledObjectMapper(), nil, constants.EmulatorProjectId)
	if err != nil {
		return nil, err
	}

	return &EmulatedDatastore{Datastore: datastoreDb, emulator: emulator}, nil
}

func startEmulator() (*emulator, error) {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Generate a random port number for Datastore emulator
		port := rand.Intn(portRangeEnd-portRangeStart) + portRangeStart
		address := fmt.Sprintf("%s:%d", emulatorHost, port)
		// Check if port is not in use already and try again
		_, err := net.DialTimeout("tcp", address, pauseDuration)
		if err == nil {
			logger.Errorf("Address '%s' is already in use", address)
			continue
		}

		cmd := exec.Command("gcloud",
			"beta", "emulators", "datastore", "start",
			fmt.Sprintf("--host-port=%s", address),
			fmt.Sprintf("--project=%s", constants.EmulatorProjectId),
			"--consistency=1",
			"--no-store-on-disk")

		// Datastore emulator start several child processes, to be able to terminate them all we need to set pgid
		// for the process group
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		err = cmd.Start()
		if err != nil {
			return nil, err
		}

		processExited := false
		go func() {
			err := cmd.Wait()
			if err != nil {
				logger.Errorf("Emulator exited with error: %s", err.Error())
			}
			processExited = true
		}()

		// Give some time for command to exit in case something is wrong
		time.Sleep(startupDuration)

		ok := func() bool {
			workTicker := time.NewTicker(pauseDuration)
			defer workTicker.Stop()
			globalTicker := time.NewTicker(waitDuration)
			defer workTicker.Stop()
			for {
				select {
				case <-workTicker.C:
					if _, err = net.DialTimeout("tcp", address, pauseDuration); err == nil {
						return true
					}
				case <-globalTicker.C:
					return false
				}
			}
		}()

		if ok && !processExited {
			return &emulator{
				cmd:  cmd,
				Host: emulatorHost,
				Port: port}, nil
		}

		// Timed out waiting for emulator to come online, kill the process and try again
		if cmd.Process != nil && !processExited {
			pgid, err := syscall.Getpgid(cmd.Process.Pid)
			if err != nil {
				return nil, err
			}
			err = syscall.Kill(-pgid, syscall.SIGTERM)
			if err != nil {
				return nil, err
			}
		}
	}

	return nil, fmt.Errorf("max attempts count (%d) reached", maxAttempts)
}

func (e *emulator) GetEndpoint() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func (e *emulator) Stop() error {
	if e.cmd.Process != nil {
		// Use pgid to terminate all child processes
		pgid, err := syscall.Getpgid(e.cmd.Process.Pid)
		if err != nil {
			return err
		}
		err = syscall.Kill(-pgid, syscall.SIGTERM)
		if err != nil {
			return err
		}
	}
	// cmd.Process == nil indicates that process was not started. Do not make an error an attempt to stop not started process
	return nil
}
