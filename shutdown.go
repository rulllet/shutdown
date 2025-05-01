// Package shutdown provides a graceful shutdown mechanism with priority support
package shutdown

import (
	"context"
	"log"
	"os"
	"os/signal"
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

// Closer manages the graceful shutdown process
type Closer struct {
	mu            sync.Mutex            // Protects access to handlers and closed
	handlers      []handlerWithPriority // List of registered handlers
	timeout       time.Duration         // Timeout for each individual handler
	globalTimeout time.Duration         // General timeout for all handlers
	closed        bool                  // Flag to prevent Close from being called again
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
		wg     sync.WaitGroup
		hasErr atomic.Bool
	)

	wg.Add(len(c.handlers))

	// Execute handlers in order of priority
	for _, hp := range c.handlers {
		go func(h Handler) {
			defer wg.Done()

			hCtx, hCancel := context.WithTimeout(ctx, c.timeout)
			defer hCancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() {
					if r := recover(); r != nil {
						log.Printf("PANIC in handler (priority %d): %v", hp.priority, r)
						hasErr.Store(true)
					}
				}()
				if err := h(hCtx); err != nil {
					log.Printf("Handler error (priority %d): %v", hp.priority, err)
					hasErr.Store(true)
				}
			}()

			select {
			case <-done:
			case <-hCtx.Done():
				log.Printf("Handler timeout (priority %d) after %v", hp.priority, c.timeout)
				hasErr.Store(true)
			}
		}(hp.handler)
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
