package shutdown_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rulllet/shutdown"
)

// Basic registration and execution test - checks that handlers are actually executed
func TestCloser_Register(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)

	var executed bool
	c.Register(func(ctx context.Context) error {
		executed = true
		return nil
	})

	c.Close()

	if !executed {
		t.Error("Handler was not executed")
	}
}

// Priority check - make sure handlers are executed in the correct order
func TestCloser_PriorityOrder(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)

	var wg sync.WaitGroup
	wg.Add(3)

	var actualOrder []int
	var mu sync.Mutex

	c.RegisterWithPriority(func(ctx context.Context) error {
		defer wg.Done()
		mu.Lock()
		actualOrder = append(actualOrder, shutdown.PriorityNormal)
		mu.Unlock()
		return nil
	}, shutdown.PriorityNormal)

	c.RegisterWithPriority(func(ctx context.Context) error {
		defer wg.Done()
		mu.Lock()
		actualOrder = append(actualOrder, shutdown.PriorityCritical)
		mu.Unlock()
		return nil
	}, shutdown.PriorityCritical)

	c.RegisterWithPriority(func(ctx context.Context) error {
		defer wg.Done()
		mu.Lock()
		actualOrder = append(actualOrder, shutdown.PriorityLow)
		mu.Unlock()
		return nil
	}, shutdown.PriorityLow)

	c.Close()
	wg.Wait()

	if len(actualOrder) == 0 || actualOrder[0] != shutdown.PriorityCritical {
		t.Errorf("Critical handler should execute first")
	}

	if len(actualOrder) > 0 && actualOrder[len(actualOrder)-1] != shutdown.PriorityLow {
		t.Errorf("Low priority handler should execute last")
	}
}

// Error handling - tests returning an error code when problems occur
func TestCloser_HandlerError(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)

	c.Register(func(ctx context.Context) error {
		return errors.New("test error")
	})

	code := c.Close()

	if code != 1 {
		t.Error("Expected error code 1, got", code)
	}
}

// Panic Handling - checks for panic recovery in the handler
func TestCloser_HandlerPanic(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)

	c.Register(func(ctx context.Context) error {
		panic("test panic")
	})

	code := c.Close()

	if code != 1 {
		t.Error("Expected error code 1 after panic, got", code)
	}
}

// Timeouts - tests the behavior when the execution time is exceeded
func TestCloser_Timeout(t *testing.T) {
	c := shutdown.New(100*time.Millisecond, 200*time.Millisecond)

	c.Register(func(ctx context.Context) error {
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	code := c.Close()

	if code != 1 {
		t.Error("Expected error code 1 after timeout, got", code)
	}
}

// Call Close again - checks for idempotency
func TestCloser_DoubleClose(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)

	firstCall := c.Close()
	secondCall := c.Close()

	if firstCall != 0 || secondCall != 0 {
		t.Error("Close should return 0 when called multiple times")
	}
}

// Late registration - handlers after Close should not be executed
func TestCloser_RegisterAfterClose(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)
	c.Close()

	var executed bool
	c.Register(func(ctx context.Context) error {
		executed = true
		return nil
	})

	if executed {
		t.Error("Handler should not be executed after Close")
	}
}

// Concurrent registration - checks for safety when used in parallel
func TestCloser_ConcurrentRegistration(t *testing.T) {
	c := shutdown.New(time.Second, 2*time.Second)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Register(func(ctx context.Context) error {
				return nil
			})
		}()
	}

	wg.Wait()
	c.Close()
}

// Global timeout - checks the total execution time
func TestCloser_GlobalTimeout(t *testing.T) {
	c := shutdown.New(time.Second, 500*time.Millisecond)

	for i := 0; i < 5; i++ {
		c.Register(func(ctx context.Context) error {
			time.Sleep(600 * time.Millisecond)
			return nil
		})
	}

	code := c.Close()

	if code != 1 {
		t.Error("Expected error code 1 after global timeout, got", code)
	}
}

// Context pass - checks if context was cancelled correctly
func TestCloser_ContextPropagation(t *testing.T) {
	c := shutdown.New(100*time.Millisecond, 200*time.Millisecond)

	c.Register(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
			return nil
		}
	})

	code := c.Close()

	if code != 1 {
		t.Error("Expected error code 1 when context is canceled, got", code)
	}
}
