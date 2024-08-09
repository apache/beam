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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/container/tools"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx/expansionx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
)

const pipLogFlushInterval time.Duration = 15 * time.Second
const unrecoverableURL string = "https://beam.apache.org/documentation/sdks/python-unrecoverable-errors/index.html#pip-dependency-resolution-failures"

// pipInstallRequirements installs the given requirement, if present.
func pipInstallRequirements(ctx context.Context, logger *tools.Logger, files []string, dir, name string) error {
	pythonVersion, err := expansionx.GetPythonVersion()
	if err != nil {
		return err
	}
	bufLogger := tools.NewBufferedLoggerWithFlushInterval(ctx, logger, pipLogFlushInterval)
	for _, file := range files {
		if file == name {
			// We run the install process in two rounds in order to avoid as much
			// as possible PyPI downloads. In the first round the --find-links
			// option will make sure that only things staged in the worker will be
			// used without following their dependencies.
			args := []string{"-m", "pip", "install", "-r", filepath.Join(dir, name), "--no-cache-dir", "--disable-pip-version-check", "--no-index", "--no-deps", "--find-links", dir}
			if err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...); err != nil {
				bufLogger.Printf(ctx, "Some packages could not be installed solely from the requirements cache. Installing packages from PyPI.")
			}
			// The second install round opens up the search for packages on PyPI and
			// also installs dependencies. The key is that if all the packages have
			// been installed in the first round then this command will be a no-op.
			args = []string{"-m", "pip", "install", "-r", filepath.Join(dir, name), "--no-cache-dir", "--disable-pip-version-check", "--find-links", dir}
			err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...)
			if err != nil {
				bufLogger.FlushAtError(ctx)
				return fmt.Errorf("PIP failed to install dependencies, got %s. This error may be unrecoverable, see %s for more information", err, unrecoverableURL)
			}
			bufLogger.FlushAtDebug(ctx)
			return nil
		}
	}
	return nil
}

// isPackageInstalled checks if the given package is installed in the
// environment.
func isPackageInstalled(pkgName string) bool {
	cmd := exec.Command("python", "-m", "pip", "show", pkgName)
	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return false
		}
	}
	return true
}

// pipInstallPackage installs the given package, if present.
func pipInstallPackage(ctx context.Context, logger *tools.Logger, files []string, dir, name string, force, optional bool, extras []string) error {
	pythonVersion, err := expansionx.GetPythonVersion()
	if err != nil {
		return err
	}
	bufLogger := tools.NewBufferedLoggerWithFlushInterval(ctx, logger, pipLogFlushInterval)
	for _, file := range files {
		if file == name {
			var packageSpec = name
			if extras != nil {
				packageSpec += "[" + strings.Join(extras, ",") + "]"
			}
			if force {
				// We only use force reinstallation for packages specified using the
				// --extra_package flag.  In this case, we always want to use the
				// user-specified package, overwriting any existing package already
				// installed.  At the same time, we want to avoid reinstalling any
				// dependencies.  The "pip install" command doesn't have a clean way to do
				// this, so we do this in two steps.
				//
				// First, we use the three flags "--upgrade --force-reinstall --no-deps"
				// to "pip install" so as to force the package to be reinstalled, while
				// avoiding reinstallation of dependencies.  Note now that if any needed
				// dependencies were not installed, they will still be missing.
				//
				// Next, we run "pip install" on the package without these flags.  Since the
				// installed version will match the package specified, the package itself
				// will not be reinstalled, but its dependencies will now be resolved and
				// installed if necessary.  This achieves our goal outlined above.
				args := []string{"-m", "pip", "install", "--no-cache-dir", "--disable-pip-version-check", "--upgrade", "--force-reinstall", "--no-deps",
					filepath.Join(dir, packageSpec)}
				err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...)
				if err != nil {
					bufLogger.FlushAtError(ctx)
					return fmt.Errorf("PIP failed to install dependencies, got %s. This error may be unrecoverable, see %s for more information", err, unrecoverableURL)
				} else {
					bufLogger.FlushAtDebug(ctx)
				}
				args = []string{"-m", "pip", "install", "--no-cache-dir", "--disable-pip-version-check", filepath.Join(dir, packageSpec)}
				err = execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...)
				if err != nil {
					bufLogger.FlushAtError(ctx)
					return fmt.Errorf("PIP failed to install dependencies, got %s. This error may be unrecoverable, see %s for more information", err, unrecoverableURL)
				}
				bufLogger.FlushAtDebug(ctx)
				return nil
			}

			// Case when we do not perform a forced reinstall.
			args := []string{"-m", "pip", "install", "--no-cache-dir", "--disable-pip-version-check", filepath.Join(dir, packageSpec)}
			err := execx.ExecuteEnvWithIO(nil, os.Stdin, bufLogger, bufLogger, pythonVersion, args...)
			if err != nil {
				bufLogger.FlushAtError(ctx)
				return fmt.Errorf("PIP failed to install dependencies, got %s. This error may be unrecoverable, see %s for more information", err, unrecoverableURL)
			}
			bufLogger.FlushAtDebug(ctx)
			return nil
		}
	}
	if optional {
		return nil
	}
	return errors.New("package '" + name + "' not found")
}

// installExtraPackages installs all the packages declared in the extra
// packages manifest file.
func installExtraPackages(ctx context.Context, logger *tools.Logger, files []string, extraPackagesFile, dir string) error {
	bufLogger := tools.NewBufferedLoggerWithFlushInterval(ctx, logger, pipLogFlushInterval)
	// First check that extra packages manifest file is present.
	for _, file := range files {
		if file != extraPackagesFile {
			continue
		}

		// Found the manifest. Install extra packages.
		manifest, err := os.ReadFile(filepath.Join(dir, extraPackagesFile))
		if err != nil {
			return fmt.Errorf("failed to read extra packages manifest file: %v", err)
		}

		s := bufio.NewScanner(bytes.NewReader(manifest))
		s.Split(bufio.ScanLines)

		for s.Scan() {
			extraPackage := s.Text()
			bufLogger.Printf(ctx, "Installing extra package: %s", extraPackage)
			if err = pipInstallPackage(ctx, logger, files, dir, extraPackage, true, false, nil); err != nil {
				return fmt.Errorf("failed to install extra package %s: %v", extraPackage, err)
			}
		}
		return nil
	}
	return nil
}

func findBeamSdkWhl(ctx context.Context, logger *tools.Logger, files []string, acceptableWhlSpecs []string) string {
	bufLogger := tools.NewBufferedLoggerWithFlushInterval(ctx, logger, pipLogFlushInterval)
	for _, file := range files {
		if strings.HasPrefix(file, "apache_beam") {
			for _, s := range acceptableWhlSpecs {
				if strings.HasSuffix(file, s) {
					bufLogger.Printf(ctx, "Found Apache Beam SDK wheel: %v", file)
					return file
				}
			}
		}
	}
	return ""
}

// InstallSdk installs Beam SDK: First, we try to find a compiled
// wheel distribution of Apache Beam among staged files. If we find it, we
// assume that the pipleine was started with the Beam SDK found in the wheel
// file, and we try to install it. If not successful, we fall back to installing
// SDK from source tarball provided in sdkSrcFile.
func installSdk(ctx context.Context, logger *tools.Logger, files []string, workDir string, sdkSrcFile string, acceptableWhlSpecs []string, required bool) error {
	sdkWhlFile := findBeamSdkWhl(ctx, logger, files, acceptableWhlSpecs)
	bufLogger := tools.NewBufferedLoggerWithFlushInterval(ctx, logger, pipLogFlushInterval)
	if sdkWhlFile != "" {
		// by default, pip rejects to install wheel if same version already installed
		isDev := strings.Contains(sdkWhlFile, ".dev")
		err := pipInstallPackage(ctx, logger, files, workDir, sdkWhlFile, isDev, false, []string{"gcp"})
		if err == nil {
			return nil
		}
		bufLogger.Printf(ctx, "Could not install Apache Beam SDK from a wheel: %v, proceeding to install SDK from source tarball.", err)
	}
	if !required {
		_, err := os.Stat(filepath.Join(workDir, sdkSrcFile))
		if os.IsNotExist(err) {
			return nil
		}
	}
	err := pipInstallPackage(ctx, logger, files, workDir, sdkSrcFile, false, false, []string{"gcp"})
	return err
}
