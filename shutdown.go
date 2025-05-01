// Package shutdown provides a graceful shutdown mechanism with priority support
package shutdown

import (
	"context"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Handler priorities
const (
	PriorityCritical = 0 // Highest priority (eg saving data)
	PriorityHigh     = 1
	PriorityNormal   = 2
	PriorityLow      = 3 // Lowest priority
)

// Handler defines the type of handler function that is executed upon completion.
// Accepts a context with a timeout and returns an error (if any).
type Handler func(ctx context.Context) error

// handlerWithPriority contains the handler and its priority
type handlerWithPriority struct {
	handler  Handler
	priority int
}

// HandlerInfo содержит полную информацию о выполнении обработчика
type HandlerInfo struct {
	FunctionName string        // Имя функции (извлекается автоматически)
	Priority     int           // Приоритет обработчика
	Success      bool          // Успешность выполнения
	Error        error         // Ошибка, если была
	Panic        interface{}   // Паника, если была
	Duration     time.Duration // Время выполнения
}

// Closer manages the graceful shutdown process
type Closer struct {
	mu            sync.Mutex            // Protects access to handlers and closed
	handlers      []handlerWithPriority // List of registered handlers
	timeout       time.Duration         // Timeout for each individual handler
	globalTimeout time.Duration         // General timeout for all handlers
	closed        bool                  // Flag to prevent Close from being called again
	onComplete    func(results []HandlerInfo)
}

// New creates a new instance of Closer.
// Parameters:
// - handlerTimeout: maximum execution time of one handler
// - globalTimeout: maximum total time for all handlers to complete
func New(handlerTimeout, globalTimeout time.Duration) *Closer {
	return &Closer{
		timeout:       handlerTimeout,
		globalTimeout: globalTimeout,
	}
}

// Register adds a handler with normal priority
func (c *Closer) Register(h Handler) {
	c.RegisterWithPriority(h, PriorityNormal)
}

// RegisterWithPriority adds a handler with the specified priority
func (c *Closer) RegisterWithPriority(h Handler, priority int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		log.Println("Warning: Register called after Close")
		return
	}

	c.handlers = append(c.handlers, handlerWithPriority{
		handler:  h,
		priority: priority,
	})
}

// Batch registers a list of handlers with the specified priority
//
//	closer := shutdown.New(5*time.Second, 15*time.Second)
//	dbHandlers := []shutdown.Handler{
//	    func(ctx context.Context) error { return db.CloseConnections() },
//	    func(ctx context.Context) error { return db.FlushBuffers() },
//	    func(ctx context.Context) error { return db.Backup(ctx) },
//	}
//
// 	closer.Batch(dbHandlers, shutdown.PriorityCritical)
func (c *Closer) Batch(handlers []Handler, priority int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		log.Println("Warning: Batch registration called after Close")
		return
	}

	for _, h := range handlers {
		c.handlers = append(c.handlers, handlerWithPriority{
			handler:  h,
			priority: priority,
		})
	}
}

// WithDefaults registers handlers with default parameters
//
//	closer := shutdown.New(5*time.Second, 15*time.Second)
//	closer.WithDefaults(
//
//	metrics.Flush,
//	notifications.SendShutdownAlert,
//	debug.DumpState,
//
//	)
func (c *Closer) WithDefaults(handlers ...Handler) {
	c.Batch(handlers, PriorityNormal)
}

// Sequence registers lists of dependent handler chains:
//
//	closer := shutdown.New(5*time.Second, 15*time.Second)
//
//	The handlers will be executed in the order A -> B -> C
//
//	closer.Sequence([]shutdown.Handler{handlerA, handlerB, handlerC}, PriorityNormal)
func (c *Closer) Sequence(handlers []Handler, priority int) {
	for i := len(handlers) - 1; i >= 0; i-- {
		c.RegisterWithPriority(handlers[i], priority)
	}
}

// Close executes all handlers in priority order and returns an exit code.
// Returns:
// - 0: if all handlers succeeded
// - 1: if there were errors or timeouts
func (c *Closer) Close() int {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	c.closed = true
	c.mu.Unlock()

	// Sort handlers by priority (most important first)
	sort.Slice(c.handlers, func(i, j int) bool {
		return c.handlers[i].priority < c.handlers[j].priority
	})

	ctx, cancel := context.WithTimeout(context.Background(), c.globalTimeout)
	defer cancel()

	var (
		wg          sync.WaitGroup
		hasErr      atomic.Bool
		results     = make([]HandlerInfo, len(c.handlers))
		resultsLock sync.Mutex
	)

	wg.Add(len(c.handlers))

	// Execute handlers in order of priority
	for i, hp := range c.handlers {
		go func(idx int, h Handler, priority int) {
			defer wg.Done()

			startTime := time.Now()
			hCtx, hCancel := context.WithTimeout(ctx, c.timeout)
			defer hCancel()

			var handlerErr error
			var panicObj interface{}
			success := true

			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() {
					if r := recover(); r != nil {
						panicObj = r
						success = false
						log.Printf("PANIC in handler (priority %d): %v", hp.priority, r)
						hasErr.Store(true)
					}
				}()
				if err := h(hCtx); err != nil {
					handlerErr = err
					success = false
					log.Printf("Handler error (priority %d): %v", hp.priority, err)
					hasErr.Store(true)
				}
			}()

			select {
			case <-done:
			case <-hCtx.Done():
				if handlerErr == nil {
					handlerErr = context.DeadlineExceeded
				}
				success = false
				log.Printf("Handler timeout (priority %d) after %v", hp.priority, c.timeout)
				hasErr.Store(true)
			}

			// We get the function name via reflection
			funcName := runtime.FuncForPC(reflect.ValueOf(h).Pointer()).Name()

			// Simplify the function name (remove the package path)
			if shortName := getShortFuncName(funcName); shortName != "" {
				funcName = shortName
			}

			resultsLock.Lock()
			results[idx] = HandlerInfo{
				FunctionName: funcName,
				Priority:     priority,
				Success:      success,
				Error:        handlerErr,
				Panic:        panicObj,
				Duration:     time.Since(startTime),
			}
			resultsLock.Unlock()
		}(i, hp.handler, hp.priority)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All handlers completed")
	case <-ctx.Done():
		log.Println("Global timeout reached, exiting forcefully")
		hasErr.Store(true)
	}

	if c.onComplete != nil {
		c.onComplete(results)
	}

	if hasErr.Load() {
		return 1
	}
	return 0
}

// Wait waits for termination signals and starts the graceful shutdown process.
// Supported signals:
// - SIGINT (Ctrl+C)
// - SIGTERM (termination signal)
// - SIGHUP (configuration reload)
//
// After executing the handlers, calls os.Exit with the appropriate code.
func Wait(c *Closer) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down...\n", sig)

	code := c.Close()
	os.Exit(code)
}

// OnComplete sets up a callback to receive the results of all handlers
func (c *Closer) OnComplete(fn func(results []HandlerInfo)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onComplete = fn
}

// getShortFuncName extracts the short name of a function from its full path
func getShortFuncName(fullName string) string {
	for i := len(fullName) - 1; i >= 0; i-- {
		if fullName[i] == '.' {
			return fullName[i+1:]
		}
	}
	return fullName
}
