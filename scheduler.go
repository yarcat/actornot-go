package actornot

import "sync"

// Scheduler is an interface that defines the methods for running tasks.
// It have serial and concurrent implementations.
type Scheduler interface {
	Schedule(func())
}

// SchedulerFunc is a function that implements the Scheduler interface.
type SchedulerFunc func(func())

func (f SchedulerFunc) Schedule(task func()) { f(task) }

// ScheduleWaiter is an interface that defines the methods for scheduling tasks
// and waiting for them to complete.
type ScheduleWaiter interface {
	Scheduler
	Wait()
}

// ConcurrentScheduler implements the SchedulerWaiter interface.
type ConcurrentScheduler struct{ wg sync.WaitGroup }

// NewConcurrentScheduler creates a new ConcurrentScheduler.
func NewConcurrentScheduler() *ConcurrentScheduler { return new(ConcurrentScheduler) }

// Schedule schedules a task to be run concurrently.
func (s *ConcurrentScheduler) Schedule(task func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		task()
	}()
}

// Wait waits for all scheduled tasks to complete. It blocks until all tasks are done.
// No tasks can be scheduled after this method is called.
func (s *ConcurrentScheduler) Wait() { s.wg.Wait() }
