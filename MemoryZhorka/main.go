package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

const (
	chunkSize   = 64 * 1024 * 1024
	targetBytes = 3 * 1024 * 1024 * 1024
)

var eating atomic.Bool

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":           "ok",
			"targetBytes":      targetBytes,
			"allocationActive": eating.Load(),
		})
	})
	mux.HandleFunc("POST /eat-memory", func(w http.ResponseWriter, _ *http.Request) {
		if !eating.CompareAndSwap(false, true) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "memory allocation is already running"})
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]any{
			"status":      "allocation started",
			"targetBytes": targetBytes,
			"targetGiB":   3,
		})

		go eatMemory(logger)
	})

	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	logger.Info("MemoryZhorka started", "address", server.Addr, "targetBytes", targetBytes)
	if err := server.ListenAndServe(); err != nil {
		logger.Error("http server stopped", "error", err)
		os.Exit(1)
	}
}

func eatMemory(logger *slog.Logger) {
	chunks := make([][]byte, 0, targetBytes/chunkSize)
	allocated := 0
	for allocated < targetBytes {
		chunk := make([]byte, chunkSize)
		for offset := 0; offset < len(chunk); offset += 4096 {
			chunk[offset] = 1
		}
		chunks = append(chunks, chunk)
		allocated += len(chunk)
		logger.Info("memory consumed", "allocatedBytes", allocated, "allocatedMiB", allocated/(1024*1024))
		time.Sleep(50 * time.Millisecond)
	}

	logger.Info("memory target reached", "allocatedBytes", allocated)
	select {}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
