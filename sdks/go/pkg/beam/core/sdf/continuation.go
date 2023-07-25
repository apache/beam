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

package sdf

import "time"

// ProcessContinuation is an interface used to signal that a splittable DoFn should be
// split and resumed at a later time. The ProcessContinuation can be returned from
// a DoFn when it returns, either complete or needing to be resumed.
type ProcessContinuation interface {
	// ShouldResume returns a boolean indicating whether the process should be
	// resumed at a later time.
	ShouldResume() bool

	// ResumeDelay returns a suggested time.Duration to wait before resuming the
	// process. The runner is not guaranteed to follow this suggestion.
	ResumeDelay() time.Duration
}

// defaultProcessContinuation is the SDK-default implementation of the ProcessContinuation
// interface, encapsulating the basic behavior necessary to resume a process later.
type defaultProcessContinuation struct {
	resumes     bool
	resumeDelay time.Duration
}

// ShouldResume returns whether or not the DefaultProcessContinuation should lead to the
// process being resumed.
func (p *defaultProcessContinuation) ShouldResume() bool {
	return p.resumes
}

// ResumeDelay returns the suggested duration that should pass before the process is resumed.
// If the process should not be resumed, the value returned here does not matter.
func (p *defaultProcessContinuation) ResumeDelay() time.Duration {
	return p.resumeDelay
}

// StopProcessing returns a ProcessContinuation that will not resume the process
// later.
func StopProcessing() ProcessContinuation {
	return &defaultProcessContinuation{resumes: false, resumeDelay: 0 * time.Second}
}

// ResumeProcessingIn returns a ProcessContinuation that will resume the process
// later with a suggested delay passed as a time.Duration.
func ResumeProcessingIn(delay time.Duration) ProcessContinuation {
	return &defaultProcessContinuation{resumes: true, resumeDelay: delay}
}
