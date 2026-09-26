package main

import "testing"

func TestRetryDelaySeconds(t *testing.T) {
	tests := []struct {
		attempt int
		want    int
	}{
		{attempt: 0, want: 1},
		{attempt: 1, want: 1},
		{attempt: 2, want: 2},
		{attempt: 3, want: 4},
		{attempt: 6, want: 32},
		{attempt: 7, want: 60},
		{attempt: 20, want: 60},
	}

	for _, tt := range tests {
		if got := retryDelaySeconds(tt.attempt); got != tt.want {
			t.Fatalf("attempt %d: got %d, want %d", tt.attempt, got, tt.want)
		}
	}
}
