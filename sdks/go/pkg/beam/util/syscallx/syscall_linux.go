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

//go:build linux
// +build linux

package syscallx

import "golang.org/x/sys/unix"

// PhysicalMemorySize returns the total physical memory size.
func PhysicalMemorySize() (uint64, error) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, err
	}
	return info.Totalram, nil
}

// FreeDiskSpace returns the free disk space for a given path.
func FreeDiskSpace(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

// SetProcessMemoryCeiling sets current and max process memory limit.
func SetProcessMemoryCeiling(softCeiling, hardCeiling uint64) error {
	var rLimit unix.Rlimit

	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		return err
	}

	rLimit.Max = hardCeiling
	rLimit.Cur = softCeiling

	return unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
}
