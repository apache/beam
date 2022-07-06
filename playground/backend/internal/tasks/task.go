package tasks

import (
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"github.com/procyon-projects/chrono"
	"time"
)

type ScheduledTask struct {
	ctx           context.Context
	taskScheduler chrono.TaskScheduler
}

func New(ctx context.Context) *ScheduledTask {
	return &ScheduledTask{ctx: ctx, taskScheduler: chrono.NewDefaultTaskScheduler()}
}

func (st *ScheduledTask) StartRemovingExtraSnippets(cron string, dayDiff int32, db db.Database) error {
	_, err := st.taskScheduler.ScheduleWithCron(func(ctx context.Context) {
		logger.Info("ScheduledTask: StartRemovingExtraSnippets() is running...\n")
		startDate := time.Now()
		if err := db.DeleteUnusedSnippets(ctx, dayDiff); err != nil {
			logger.Errorf("ScheduledTask: StartRemovingExtraSnippets() error during deleting unused snippets, err: %s\n", err.Error())
		}
		diffTime := time.Now().Sub(startDate).Milliseconds()
		logger.Infof("ScheduledTask: StartRemovingExtraSnippets() finished, work time: %d ms\n", diffTime)
	}, cron, chrono.WithLocation("UTC"))

	if err != nil {
		logger.Errorf("ScheduledTask: StartRemovingExtraSnippets() error during task running, err: %s\n", err.Error())
		return err
	}
	return nil
}
