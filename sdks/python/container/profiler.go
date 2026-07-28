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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/container/tools"
)

type profilerConfigKeyType struct{}

var profilerConfigKey profilerConfigKeyType

// ProfilerConfig holds all pre-computed profiling parameters.
type ProfilerConfig struct {
	Enabled                bool
	Agent                  string
	ExtraArgs              []string
	ExtraEnvVars           []string
	Location               string
	TempLocation           string
	BaseTempDir            string
	StopSentinelPath       string
	GcsDestPath            string
	UploadIntervalSec      int
	StopAfterSec           int
	StopAfterCrash         bool
	PostprocessIntervalSec int
}

// setupProfilerConfig parses PipelineOptionsData and stores a resolved ProfilerConfig in the context.
func setupProfilerConfig(ctx context.Context, logger *tools.Logger, opts *PipelineOptionsData) context.Context {
	agent := opts.Options.ProfilerAgent
	if agent == "" {
		return ctx
	}

	baseTempDir := opts.Options.ProfileTempLocation
	if baseTempDir == "" {
		baseTempDir = filepath.Join(*semiPersistDir, "profiles")
	}

	jobId := opts.Options.JobId
	if jobId == "" {
		jobId = "BEAM_JOB"
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "default-worker"
	}

	tempLocation := filepath.Join(baseTempDir, jobId, hostname)
	sentinelPath := filepath.Join(tempLocation, fmt.Sprintf(".profiler_disengaged_%s_%s", jobId, hostname))

	var gcsDestPath string
	if strings.HasPrefix(opts.Options.ProfileLocation, "gs://") {
		gcsDestPath = strings.TrimSuffix(opts.Options.ProfileLocation, "/")
	}

	config := &ProfilerConfig{
		Enabled:                true,
		Agent:                  agent,
		ExtraArgs:              opts.Options.ProfilerExtraArgs,
		ExtraEnvVars:           opts.Options.ProfilerExtraEnvVars,
		Location:               opts.Options.ProfileLocation,
		BaseTempDir:            baseTempDir,
		TempLocation:           tempLocation,
		StopSentinelPath:       sentinelPath,
		GcsDestPath:            gcsDestPath,
		UploadIntervalSec:      opts.Options.ProfileUploadIntervalSec,
		StopAfterSec:           opts.Options.ProfilerStopAfterSec,
		StopAfterCrash:         opts.Options.ProfilerStopAfterCrash,
		PostprocessIntervalSec: opts.Options.ProfilePostprocessIntervalSec,
	}

	return context.WithValue(ctx, profilerConfigKey, config)
}

// getProfilerConfig extracts the ProfilerConfig from the context.
func getProfilerConfig(ctx context.Context) *ProfilerConfig {
	if cfg, ok := ctx.Value(profilerConfigKey).(*ProfilerConfig); ok {
		return cfg
	}
	return nil
}

// startProfilerBackgroundTasks initializes profiling locations and runs background tasks (GCS sync, post-processing loops) if profiling is enabled.
func startProfilerBackgroundTasks(ctx context.Context, logger *tools.Logger) {
	pcfg := getProfilerConfig(ctx)
	if pcfg == nil {
		return
	}

	logger.Printf(ctx, "Worker will be configured with profiler agent enabled.")
	logger.Printf(ctx, "ProfilerAgent: %v", pcfg.Agent)
	logger.Printf(ctx, "ProfilerExtraArgs: %v", pcfg.ExtraArgs)
	logger.Printf(ctx, "ProfilerExtraEnvVars: %v", pcfg.ExtraEnvVars)
	logger.Printf(ctx, "ProfileLocation: %v", pcfg.Location)
	logger.Printf(ctx, "ProfileTempLocation: %v", pcfg.BaseTempDir)
	logger.Printf(ctx, "ProfileUploadIntervalSec: %v", pcfg.UploadIntervalSec)
	logger.Printf(ctx, "ProfilerStopAfterSec: %v", pcfg.StopAfterSec)
	logger.Printf(ctx, "ProfilerStopAfterCrash: %v", pcfg.StopAfterCrash)
	logger.Printf(ctx, "ProfilePostprocessIntervalSec: %v", pcfg.PostprocessIntervalSec)
	if err := os.MkdirAll(pcfg.TempLocation, 0755); err != nil {
		logger.Warnf(ctx, "Failed to create ProfileTempLocation: %v", err)
	}

	if pcfg.GcsDestPath != "" {
		if _, err := exec.LookPath("gcloud"); err != nil {
			logger.Errorf(ctx, "gcloud is not available, profiles will not be uploaded.")
		} else {
			if pcfg.UploadIntervalSec > 0 {
				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Duration(pcfg.UploadIntervalSec) * time.Second):
							// TODO(tvalentyn): Consider a periodic cleanup as well to save local disk space.
							syncProfilesToGCS(ctx, logger, pcfg.BaseTempDir, pcfg.GcsDestPath)
						}
					}
				}()
			}
		}
	}

	if pcfg.Agent == "memray" {
		go postProcessProfilesLoop(ctx, logger, pcfg)
	}

	if pcfg.Agent == "coredump" {
		go monitorCoredumpsLoop(ctx, logger, pcfg)
	}

}

// maybeWithProfiler builds the execution arguments and environment variables if profiling is enabled and active.
func maybeWithProfiler(
	ctx context.Context,
	logger *tools.Logger,
	workerId string,
	currentProg string,
	currentArgs []string,
	currentEnv map[string]string,
) (string, []string, map[string]string, bool) {
	pcfg := getProfilerConfig(ctx)
	if pcfg == nil {
		return currentProg, currentArgs, currentEnv, false
	}

	if _, err := os.Stat(pcfg.StopSentinelPath); err == nil {
		return currentProg, currentArgs, currentEnv, false
	}

	prog := currentProg
	var args []string
	// Copy env
	env := make(map[string]string)
	for k, v := range currentEnv {
		env[k] = v
	}

	if pcfg.Agent == "memray" {
		timeSuffix := time.Now().Format("20060102150405")
		memrayFile := filepath.Join(pcfg.TempLocation, fmt.Sprintf("memray-%s-%s.bin", workerId, timeSuffix))
		args = []string{"-m", "memray", "run"}
		args = append(args, pcfg.ExtraArgs...)
		args = append(args, "-o", memrayFile, "-m", sdkHarnessEntrypoint)
	} else if pcfg.Agent == "tcmalloc" {
		tcmallocHeapPath := filepath.Join(pcfg.TempLocation, fmt.Sprintf("tcmalloc-%s", workerId))
		existingPreload := os.Getenv("LD_PRELOAD")
		if existingPreload != "" {
			env["LD_PRELOAD"] = existingPreload + ":libtcmalloc.so.4"
		} else {
			env["LD_PRELOAD"] = "libtcmalloc.so.4"
		}
		env["HEAPPROFILE"] = tcmallocHeapPath
		args = currentArgs
	} else if pcfg.Agent == "coredump" {
		// No wrapping of the executable is needed for coredump analysis.
		args = currentArgs
	} else {
		prog = pcfg.Agent
		args = append(append([]string{}, pcfg.ExtraArgs...), currentProg)
		args = append(args, currentArgs...)
	}

	for _, envVar := range pcfg.ExtraEnvVars {
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		} else {
			logger.Errorf(ctx, "Failed to parse profiler extra environment variable: %v. Expected format KEY=VALUE", envVar)
		}
	}

	return prog, args, env, true
}

// stopProfiling creates a dummy file at StopSentinelPath to signal that profiling should stop.
func stopProfiling(ctx context.Context) error {
	pcfg := getProfilerConfig(ctx)
	if pcfg == nil {
		return nil
	}
	f, err := os.Create(pcfg.StopSentinelPath)
	if err == nil {
		f.Close()
	}
	return err
}

// isProfilerDisengaged checks if the stop sentinel file exists.
func isProfilerDisengaged(pcfg *ProfilerConfig) bool {
	if _, err := os.Stat(pcfg.StopSentinelPath); err == nil {
		return true
	}
	return false
}

// syncProfilesToGCS uploads newly created local memory profiles to the designated GCS target path using gcloud storage.
func syncProfilesToGCS(ctx context.Context, logger *tools.Logger, localDir, gcsDest string) {
	entries, err := os.ReadDir(localDir)
	if err != nil || len(entries) == 0 {
		return
	}

	logger.Printf(ctx, "Syncing profiles from %s to %s", localDir, gcsDest)

	cmd := exec.CommandContext(ctx, "gcloud", "storage", "rsync", "-r", localDir, gcsDest)
	if err := cmd.Run(); err != nil {
		logger.Warnf(ctx, "Failed to sync profiles to GCS: %v", err)
	} else {
		logger.Printf(ctx, "Successfully synced profiles to GCS.")
	}
}

// postProcessProfilesLoop runs a background loop that periodically triggers profile post-processing if enabled.
func postProcessProfilesLoop(ctx context.Context, logger *tools.Logger, pcfg *ProfilerConfig) {
	if pcfg.PostprocessIntervalSec <= 0 {
		return
	}

	for {
		runPostProcessingSweep(ctx, logger, pcfg.TempLocation, pcfg.PostprocessIntervalSec)

		if isProfilerDisengaged(pcfg) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(pcfg.PostprocessIntervalSec) * time.Second):
			// Block until the sleep completes before starting the next sweep
		}
	}
}

// runPostProcessingSweep scans the profiles directory and launches sequential postprocessing for newly updated profiles.
func runPostProcessingSweep(ctx context.Context, logger *tools.Logger, profilesDir string, intervalSec int) {
	files, err := os.ReadDir(profilesDir)
	if err != nil {
		return
	}

	for _, file := range files {
		name := file.Name()
		if !strings.HasSuffix(name, ".bin") || strings.HasPrefix(name, ".") {
			continue
		}

		binPath := filepath.Join(profilesDir, name)
		binInfo, err := os.Stat(binPath)
		if err != nil || binInfo.Size() == 0 {
			continue
		}

		peakHtml := strings.TrimSuffix(binPath, ".bin") + ".html"
		leaksHtml := strings.TrimSuffix(binPath, ".bin") + "_leaks.html"

		filename := filepath.Base(binPath)
		peakReportStale := needsProcessing(binInfo, peakHtml)
		leakReportStale := needsProcessing(binInfo, leaksHtml)

		if peakReportStale || leakReportStale {
			binSizeMb := float64(binInfo.Size()) / (1024 * 1024)
			logger.Printf(ctx, "Post-processing profile %s of size %.2f MB", filename, binSizeMb)
		}

		// 1. Peak Flamegraph
		if peakReportStale {
			tmpPath := peakHtml + ".tmp"
			cmd1 := exec.CommandContext(ctx, "python", "-m", "memray", "flamegraph", "-f", "-o", tmpPath, binPath)
			if err := cmd1.Run(); err != nil {
				logger.Warnf(ctx, "Failed to generate peak flamegraph for %s: %v", filename, err)
			} else {
				if err := os.Rename(tmpPath, peakHtml); err != nil {
					logger.Warnf(ctx, "Failed to rename peak flamegraph for %s: %v", filename, err)
				} else {
					logger.Printf(ctx, "Successfully updated peak flamegraph for %s", filename)
					_ = os.Chtimes(peakHtml, binInfo.ModTime(), binInfo.ModTime())
				}
			}
		}

		// 2. Leaks Flamegraph
		if leakReportStale {
			tmpPath := leaksHtml + ".tmp"
			cmd2 := exec.CommandContext(ctx, "python", "-m", "memray", "flamegraph", "-f", "--leaks", "-o", tmpPath, binPath)
			if err := cmd2.Run(); err != nil {
				logger.Warnf(ctx, "Failed to generate leaks flamegraph for %s: %v", filename, err)
			} else {
				if err := os.Rename(tmpPath, leaksHtml); err != nil {
					logger.Warnf(ctx, "Failed to rename leaks flamegraph for %s: %v", filename, err)
				} else {
					logger.Printf(ctx, "Successfully updated leaks flamegraph for %s", filename)
					_ = os.Chtimes(leaksHtml, binInfo.ModTime(), binInfo.ModTime())
				}
			}
		}
	}
}

func needsProcessing(binInfo os.FileInfo, path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		return true
	}
	// Don't regenerate when there were no updates to the profile.
	return binInfo.ModTime().After(info.ModTime())
}

func monitorCoredumpsLoop(ctx context.Context, logger *tools.Logger, pcfg *ProfilerConfig) {
	// We require the core file pattern to be configured as "/tmp/core.%e.%p" via pipeline
	// options experiments (which is automatically set by the Python SDK). This ensures
	// that core dumps are written in /tmp/ with a prefix matching "core.".
	coreDir := "/tmp"
	interval := 5 * time.Second
	if pcfg.PostprocessIntervalSec > 0 {
		interval = time.Duration(pcfg.PostprocessIntervalSec) * time.Second
	}
	logger.Printf(ctx, "Monitoring directory %s for core dumps matching prefix core. every %v", coreDir, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processNewCoredumps(ctx, logger, pcfg, coreDir)
			if isProfilerDisengaged(pcfg) {
				return
			}
		}
	}
}

func processNewCoredumps(ctx context.Context, logger *tools.Logger, pcfg *ProfilerConfig, coreDir string) {
	files, err := os.ReadDir(coreDir)
	if err != nil {
		return
	}

	prefix := "core."

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		corePath := filepath.Join(coreDir, name)
		info, err := os.Stat(corePath)
		if err != nil {
			continue
		}

		fileAge := time.Since(info.ModTime())

		// Skip if the file was modified very recently (might still be writing)
		if fileAge < 2*time.Second {
			continue
		}

		logger.Printf(ctx, "Found core dump file: %s (%d bytes)", name, info.Size())

		// Find python executable. Since the worker might be running in a venv,
		// we look for "python" in the PATH.
		pythonProg := "python"
		if path, err := exec.LookPath("python"); err == nil {
			pythonProg = path
		}

		timeSuffix := info.ModTime().Format("20060102150405")
		newName := fmt.Sprintf("%s-%s", name, timeSuffix)
		destTxtPath := filepath.Join(pcfg.TempLocation, fmt.Sprintf("%s.txt", newName))

		shouldDelete := fileAge > 60*time.Second

		pystackPath, pystackErr := exec.LookPath("pystack")
		gdbPath, gdbErr := exec.LookPath("gdb")

		if pystackErr != nil && gdbErr != nil {
			logger.Warnf(ctx, "Core dump analysis enabled but no analysis tools found. Please install pystack (recommended) or/and gdb into the runtime environment.")
		}

		if pystackErr == nil {
			args := []string{"core"}
			if len(pcfg.ExtraArgs) > 0 {
				args = append(args, pcfg.ExtraArgs...)
			} else {
				args = append(args, "--native-last")
			}
			args = append(args, corePath, pythonProg)

			logger.Printf(ctx, "Running pystack %s", strings.Join(args, " "))
			cmd := exec.CommandContext(ctx, pystackPath, args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				logger.Warnf(ctx, "pystack failed on %s: %v. Output:\n%s", name, err, string(output))
			} else {
				if err := os.WriteFile(destTxtPath, output, 0644); err != nil {
					logger.Warnf(ctx, "Failed to write pystack output to %s: %v", destTxtPath, err)
				}
				pystackSummary := createPystackSummary(string(output))
				logger.Errorf(ctx, "Full pystack coredump analysis saved to %s.txt\nExcerpt:\n%s", newName, pystackSummary)
				shouldDelete = true
			}
		}

		if gdbErr == nil {
			gdbArgs := []string{
				"-batch",
				"-ex", "set pagination off",
				"-ex", "set trace-commands on",
				"-ex", "info sharedlibrary",
				"-ex", "info proc mappings",
				"-ex", "info threads",
				"-ex", "thread",
				"-ex", "print $_siginfo",
				"-ex", "info registers",
				"-ex", "x/10i $pc",
				"-ex", "x/16gx $rsp",
				"-ex", "bt full",
				"-ex", "thread apply all bt full",
				pythonProg,
				corePath,
			}
			logger.Printf(ctx, "Running gdb on %s using %s", name, pythonProg)
			gdbCmd := exec.CommandContext(ctx, gdbPath, gdbArgs...)
			gdbOutput, err := gdbCmd.CombinedOutput()
			destGdbPath := filepath.Join(pcfg.TempLocation, fmt.Sprintf("%s.gdb.txt", newName))
			if err != nil {
				logger.Warnf(ctx, "gdb failed on %s: %v. Output:\n%s", name, err, string(gdbOutput))
			} else {
				if err := os.WriteFile(destGdbPath, gdbOutput, 0644); err != nil {
					logger.Warnf(ctx, "Failed to write gdb output to %s: %v", destGdbPath, err)
				}
				logger.Errorf(ctx, "Full GDB coredump analysis saved to %s.gdb.txt", newName)
				shouldDelete = true
			}
		}

		if shouldDelete {
			if err := os.Remove(corePath); err != nil {
				logger.Warnf(ctx, "Failed to delete core dump %s: %v", corePath, err)
			}
		}
	}
}

func extractGILThread(output string) string {
	lines := strings.Split(output, "\n")
	var result []string
	recording := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(line, "Has the GIL") {
			recording = true
		}
		if recording {
			result = append(result, line)
			if trimmed == "" {
				break
			}
		}
	}
	if len(result) == 0 {
		return ""
	}
	return strings.Join(result, "\n")
}

func firstNLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[:n], "\n")
}

func createPystackSummary(output string) string {
	gilThreadTrace := extractGILThread(output)
	if gilThreadTrace != "" {
		return gilThreadTrace
	}
	return firstNLines(output, 100)
}
