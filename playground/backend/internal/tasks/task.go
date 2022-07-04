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
		logger.Info("ScheduledTask: startRemovingExtraSnippets() is running...")
		startDate := time.Now()
		diffTime := time.Now().Sub(startDate).Milliseconds()
		logger.Info("ScheduledTask: startRemovingExtraSnippets() finished, work time: %d", diffTime)
	}, cron)

	if err != nil {
		logger.Errorf("ScheduledTask: startRemovingExtraSnippets() error during task running, err: %s", err.Error())
		return err
	}
	return nil
}
