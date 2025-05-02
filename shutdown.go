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

// HandlerInfo contains complete information about the execution of the handler
type HandlerInfo struct {
	FunctionName string        // Function name (automatically extracted)
	Priority     int           // Handler priority
	Success      bool          // Successful execution
	Error        error         // Error if there was one
	Panic        interface{}   // Panic, if there was one
	Duration     time.Duration // lead time
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

// GroupRegister registers lists of dependent handler chains:
//
//	closer := shutdown.New(5*time.Second, 15*time.Second)
//
//	The handlers will be executed in the order A -> B -> C
//
//	closer.GroupRegister([]shutdown.Handler{handlerA, handlerB, handlerC}, PriorityNormal)
func (c *Closer) GroupRegister(handlers []Handler, priority int) {
	for i := len(handlers) - 1; i >= 0; i-- {
		c.RegisterWithPriority(handlers[i], priority)
	}
}

// Close executes all registered handlers in priority groups (highest first).
// Returns:
// - 0 if all handlers completed successfully
// - 1 if any handler failed, timed out, or panicked
func (c *Closer) Close() int {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	c.closed = true
	c.mu.Unlock()

	// Early exit if no handlers registered
	if len(c.handlers) == 0 {
		log.Println("No handlers registered")
		return 0
	}

	// Sort handlers by priority (highest first)
	sort.Slice(c.handlers, func(i, j int) bool {
		return c.handlers[i].priority < c.handlers[j].priority
	})

	ctx, cancel := context.WithTimeout(context.Background(), c.globalTimeout)
	defer cancel()

	var (
		hasErr      atomic.Bool
		results     = make([]HandlerInfo, len(c.handlers))
		resultsLock sync.Mutex
	)

	// Process handlers in priority groups
	currentPriority := c.handlers[0].priority
	startIdx := 0

	for i := 0; i <= len(c.handlers); i++ {
		// Trigger group execution when either:
		// 1. Priority changes
		// 2. We reach the end of the list
		if i == len(c.handlers) || c.handlers[i].priority != currentPriority {
			groupSize := i - startIdx
			if groupSize == 0 {
				continue
			}

			log.Printf("Executing priority group %d (%d handlers)", currentPriority, groupSize)

			var wg sync.WaitGroup
			wg.Add(groupSize)

			// Execute all handlers in current priority group
			for j := startIdx; j < i; j++ {
				go func(idx int, hp handlerWithPriority) {
					defer wg.Done()
					result := c.executeHandler(ctx, hp)

					resultsLock.Lock()
					results[idx] = result
					resultsLock.Unlock()

					if !result.Success {
						hasErr.Store(true)
					}
				}(j, c.handlers[j])
			}

			// Wait for current group to complete
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				log.Printf("Priority group %d completed", currentPriority)
			case <-ctx.Done():
				log.Printf("Global timeout reached while processing priority group %d", currentPriority)
				hasErr.Store(true)
				if c.onComplete != nil {
					c.onComplete(results)
				}
				return 1
			}

			// Move to next priority group
			if i < len(c.handlers) {
				currentPriority = c.handlers[i].priority
				startIdx = i
			}
		}
	}

	if c.onComplete != nil {
		c.onComplete(results)
	}

	if hasErr.Load() {
		return 1
	}
	return 0
}

// executeHandler runs a single handler with proper timeout and panic handling
func (c *Closer) executeHandler(ctx context.Context, hp handlerWithPriority) HandlerInfo {
	startTime := time.Now()
	hCtx, hCancel := context.WithTimeout(ctx, c.timeout)
	defer hCancel()

	var (
		handlerErr error
		panicObj   interface{}
		success    bool
	)

	// Channel to track handler completion
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				panicObj = r
				log.Printf("PANIC in handler (priority %d): %v", hp.priority, r)
			}
		}()

		if err := hp.handler(hCtx); err != nil {
			handlerErr = err
			log.Printf("Handler error (priority %d): %v", hp.priority, err)
		}
	}()

	select {
	case <-done:
		success = panicObj == nil && handlerErr == nil
	case <-hCtx.Done():
		if handlerErr == nil {
			handlerErr = context.DeadlineExceeded
		}
		log.Printf("Handler timeout (priority %d) after %v", hp.priority, c.timeout)
	}
	// We get the function name via reflection
	funcName := runtime.FuncForPC(reflect.ValueOf(hp.handler).Pointer()).Name()
	return HandlerInfo{
		FunctionName: getShortFuncName(funcName),
		Priority:     hp.priority,
		Success:      success,
		Error:        handlerErr,
		Panic:        panicObj,
		Duration:     time.Since(startTime),
	}
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
