# Graceful Shutdown Module

![Go Version](https://img.shields.io/badge/go-1.18+-blue.svg)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/yourusername/shutdown)](https://goreportcard.com/report/github.com/yourusername/shutdown)

The `shutdown` package provides a mechanism for graceful shutdown of Go applications with support for handler priorities, timeouts, and error handling.

## Features

- 🚦 Priority handler execution system
- ⏱️ Individual and global timeouts
- 🛡️ Protection against panics in handlers
- 🔒 Thread-safe implementation
- 📊 Logging of all termination events
- 🚀 Support for standard OS signals (SIGINT, SIGTERM, SIGHUP)

## Installation

```bash
go get github.com/rulllet/shutdown
```

## Usage

### Basic example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"github.com/rulllet/shutdown"
)

func main() {
	// Create an instance with timeouts:
	// - 3 seconds per handler
	// - 10 seconds total timeout
	closer := shutdown.New(3*time.Second, 10*time.Second)

	// Register handlers with different priorities
	closer.RegisterWithPriority(func(ctx context.Context) error {
		fmt.Println("We perform CRITICAL operations...")
		return nil
	}, shutdown.PriorityCritical)

	closer.Register(func(ctx context.Context) error {
		fmt.Println("Performing normal closing operations...")
		return nil
	}) // PriorityNormal by default

	// Start waiting for signals
	shutdown.Wait(closer)
}
```

### Example with HTTP server

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
	"github.com/rulllet/shutdown"
)

func main() {
	closer := shutdown.New(5*time.Second, 15*time.Second)

	// Configure the HTTP server
	server := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Server is running. Send SIGINT to shutdown.")
		}),
	}

	// Register graceful shutdown of the server
	closer.RegisterWithPriority(func(ctx context.Context) error {
		log.Println("Shutting down HTTP server...")
		return server.Shutdown(ctx)
	}, shutdown.PriorityCritical)

	// Other resources
	closer.Register(func(ctx context.Context) error {
		log.Println("Closing database connections...")
		// Here is the code for closing connections to the database
		return nil
	})

	// Start the server in a goroutine
	go func() {
		log.Println("Starting server on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for completion signals
	shutdown.Wait(closer)
}
```

## Handler Priorities

The package defines four priority levels:

```go
const (
	PriorityCritical = 0 // Highest priority
	PriorityHigh     = 1
	PriorityNormal   = 2 
	PriorityLow      = 3 // Lowest priority
)
```

Handlers are executed in order of priority (from highest to lowest). For handlers with the same priority, the order of registration is preserved.

## API Reference

### `New(handlerTimeout, globalTimeout time.Duration) *Closer`

Creates a new Closer instance.

- `handlerTimeout` - maximum execution time for one handler
- `globalTimeout` - maximum execution time for all handlers

### `Register(h Handler)`

Adds a handler with priority `PriorityNormal`.

### `RegisterWithPriority(h Handler, priority int)`

Adds a handler with the specified priority.

### `Close() int`

Executes all registered handlers and returns:
- `0` - if all handlers were successful
- `1` - if there were errors or timeouts

### `Wait(c *Closer)`

Waits for termination signals (SIGINT, SIGTERM, SIGHUP) and calls `Close()`.

4. **Log errors** inside handlers to diagnose problems

## Error Handling

- Errors in handlers are logged automatically
- Panics are intercepted and logged
- Any errors return code `1`

## License

MIT