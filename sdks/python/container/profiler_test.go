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

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestActivePidsRegistry(t *testing.T) {
	// Reset active PIDs
	activePids = nil

	activePids = append(activePids, 101)
	activePids = append(activePids, 102)

	pids := getActivePids()
	if len(pids) != 2 || pids[0] != 101 || pids[1] != 102 {
		t.Errorf("Expected active pids [101, 102], got %v", pids)
	}

	unregisterPid(101)
	pids = getActivePids()
	if len(pids) != 1 || pids[0] != 102 {
		t.Errorf("Expected active pids [102], got %v", pids)
	}

	unregisterPid(102)
	pids = getActivePids()
	if len(pids) != 0 {
		t.Errorf("Expected active pids empty, got %v", pids)
	}
}

func TestSetupProfilerConfig(t *testing.T) {
	opts := &PipelineOptionsData{
		Options: OptionsData{
			ProfilerAgent: "coredump",
			JobId:         "test-job",
		},
	}
	ctx := setupProfilerConfig(context.Background(), nil, opts)
	pcfg := getProfilerConfig(ctx)
	if pcfg == nil {
		t.Fatal("ProfilerConfig was nil")
	}

	if pcfg.Agent != "coredump" {
		t.Errorf("Expected agent coredump, got %s", pcfg.Agent)
	}
}

func TestIsProfilerDisengaged(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "disengage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sentinelPath := filepath.Join(tempDir, "stop_sentinel")
	pcfg := &ProfilerConfig{
		StopSentinelPath: sentinelPath,
	}

	if isProfilerDisengaged(pcfg) {
		t.Error("Expected profiler NOT to be disengaged before sentinel creation")
	}

	// Create sentinel file
	if err := os.WriteFile(sentinelPath, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}

	if !isProfilerDisengaged(pcfg) {
		t.Error("Expected profiler to be disengaged after sentinel creation")
	}
}

func TestCreatePystackSummary(t *testing.T) {
	t.Run("ExtractsGILThreadTrace", func(t *testing.T) {
		output := "Thread 1 (waiting):\n" +
			"  File \"worker.py\", line 10, in run\n" +
			"\n" +
			"Thread 2 (active, Has the GIL):\n" +
			"  File \"main.py\", line 42, in execute\n" +
			"  File \"db.py\", line 5, in query\n" +
			"\n" +
			"Thread 3 (idle):\n" +
			"  File \"server.py\", line 99, in listen\n"

		expected := "Thread 2 (active, Has the GIL):\n" +
			"  File \"main.py\", line 42, in execute\n" +
			"  File \"db.py\", line 5, in query\n"

		result := createPystackSummary(output)
		if result != expected {
			t.Errorf("Expected:\n%s\nGot:\n%s", expected, result)
		}
	})

	t.Run("FallbackSmallOutput", func(t *testing.T) {
		output := "Thread 1 (waiting):\n" +
			"  File \"worker.py\", line 10, in run"

		result := createPystackSummary(output)
		if result != output {
			t.Errorf("Expected identical output, got:\n%s", result)
		}
	})
}
