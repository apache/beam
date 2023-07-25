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

package tasks

import (
	"beam.apache.org/playground/backend/internal/external_functions"
	"context"
	"time"

	"github.com/procyon-projects/chrono"

	"beam.apache.org/playground/backend/internal/logger"
)

type ScheduledTask struct {
	ctx           context.Context
	taskScheduler chrono.TaskScheduler
}

func New(ctx context.Context) *ScheduledTask {
	return &ScheduledTask{ctx: ctx, taskScheduler: chrono.NewDefaultTaskScheduler()}
}

func (st *ScheduledTask) StartRemovingExtraSnippets(cron string, externalFunction external_functions.ExternalFunctions) error {
	task, err := st.taskScheduler.ScheduleWithCron(func(ctx context.Context) {
		logger.Info("ScheduledTask: StartRemovingExtraSnippets() is running...\n")
		startDate := time.Now()
		if err := externalFunction.CleanupSnippets(ctx); err != nil {
			logger.Errorf("ScheduledTask: StartRemovingExtraSnippets() error during deleting unused snippets, err: %s\n", err.Error())
		}
		diffTime := time.Now().Sub(startDate).Milliseconds()
		logger.Infof("ScheduledTask: StartRemovingExtraSnippets() finished, work time: %d ms\n", diffTime)
	}, cron, chrono.WithLocation("UTC"))

	if err != nil {
		logger.Errorf("ScheduledTask: StartRemovingExtraSnippets() error during task running. Task will be cancelled, err: %s\n", err.Error())
		if !task.IsCancelled() {
			task.Cancel()
		}
		return err
	}
	return nil
}
