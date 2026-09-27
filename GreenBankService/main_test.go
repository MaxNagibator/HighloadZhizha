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

func TestCalculateCashback(t *testing.T) {
	policy := categoryPolicy{CashbackBasisPoints: 500, MinimumAmount: 1_000, MaximumPerOperation: 2_000}
	personal := personalCashback{ExtraBasisPoints: 100, MonthlyLimit: 5_000}

	tests := []struct {
		name          string
		amount        int64
		monthlyEarned int64
		want          int64
	}{
		{name: "below category minimum", amount: 999, want: 0},
		{name: "category and personal rates", amount: 10_000, want: 600},
		{name: "operation cap", amount: 100_000, want: 2_000},
		{name: "remaining monthly cap", amount: 20_000, monthlyEarned: 4_500, want: 500},
		{name: "monthly cap exhausted", amount: 20_000, monthlyEarned: 5_000, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := calculateCashback(tt.amount, policy, personal, tt.monthlyEarned); got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}
