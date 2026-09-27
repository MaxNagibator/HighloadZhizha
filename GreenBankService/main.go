package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type app struct {
	db     *pgxpool.Pool
	redis  *redis.Client
	writer *kafka.Writer
	topic  string
	log    *slog.Logger
}

type bankEvent struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	OccurredAt    time.Time `json:"occurredAt"`
	CorrelationID string    `json:"correlationId"`
	AccountID     string    `json:"accountId"`
	Amount        int64     `json:"amount"`
	Balance       int64     `json:"balance"`
	TraceID       string    `json:"traceId"`
	Category      string    `json:"category,omitempty"`
	SourceAmount  int64     `json:"sourceAmount,omitempty"`
}

type createAccountRequest struct {
	OwnerName     string `json:"ownerName"`
	CorrelationID string `json:"correlationId"`
}

type topUpRequest struct {
	Amount        int64  `json:"amount"`
	CorrelationID string `json:"correlationId"`
	Category      string `json:"category"`
}

type withdrawalRequest struct {
	Amount        int64  `json:"amount"`
	CorrelationID string `json:"correlationId"`
}

type withdrawalResult int

const (
	withdrawalApplied withdrawalResult = iota
	withdrawalAccountNotFound
	withdrawalInsufficientFunds
	withdrawalConcurrentConflict
	maxWithdrawalAttempts = 5
)

type categoryPolicy struct {
	Code                string `json:"code"`
	CashbackBasisPoints int64  `json:"cashbackBasisPoints"`
	MinimumAmount       int64  `json:"minimumAmount"`
	MaximumPerOperation int64  `json:"maximumPerOperation"`
}

type personalCashback struct {
	ExtraBasisPoints int64
	MonthlyLimit     int64
}

var defaultCategoryPolicies = []categoryPolicy{
	{Code: "groceries", CashbackBasisPoints: 300, MinimumAmount: 500, MaximumPerOperation: 5_000},
	{Code: "transport", CashbackBasisPoints: 500, MinimumAmount: 100, MaximumPerOperation: 3_000},
	{Code: "entertainment", CashbackBasisPoints: 700, MinimumAmount: 1_000, MaximumPerOperation: 10_000},
	{Code: "other", CashbackBasisPoints: 100, MinimumAmount: 100, MaximumPerOperation: 1_000},
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://bank:bank@localhost:5432/highload?sslmode=disable"))
	if err != nil {
		logger.Error("database configuration failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err = migrate(ctx, db); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()
	if err = redisClient.Ping(ctx).Err(); err != nil {
		logger.Error("redis connection failed", "error", err)
		os.Exit(1)
	}
	if err = seedCategoryCache(ctx, redisClient); err != nil {
		logger.Error("cashback category cache initialization failed", "error", err)
		os.Exit(1)
	}

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := env("KAFKA_TOPIC", "bank.account.events")
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()

	a := &app{db: db, redis: redisClient, writer: writer, topic: topic, log: logger}
	go a.runOutboxPublisher(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("POST /accounts", a.createAccount)
	mux.HandleFunc("POST /accounts/{accountId}/top-ups", a.topUp)
	mux.HandleFunc("POST /accounts/{accountId}/withdrawals", a.withdraw)

	server := &http.Server{Addr: ":8080", Handler: requestLog(logger, mux), ReadHeaderTimeout: 5 * time.Second}
	profileServer := &http.Server{Addr: ":6060", Handler: http.DefaultServeMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("GreenBankService started", "address", server.Addr, "topic", topic)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("http server failed", "error", serveErr)
			stop()
		}
	}()
	go func() {
		logger.Info("GreenBankService profiler started", "address", profileServer.Addr)
		if serveErr := profileServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("profiler server failed", "error", serveErr)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = profileServer.Shutdown(shutdownCtx)
}

func migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
SELECT pg_advisory_xact_lock(hashtext('green-bank-schema-migration'));
CREATE TABLE IF NOT EXISTS accounts (
    id uuid PRIMARY KEY,
    owner_name text NOT NULL,
    balance bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
	version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS version bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS transactions (
    id uuid PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES accounts(id),
    transaction_type text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    balance_after bigint NOT NULL CHECK (balance_after >= 0),
    correlation_id uuid NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_transaction_type_check;
ALTER TABLE transactions ADD CONSTRAINT transactions_transaction_type_check CHECK (transaction_type IN ('TOP_UP', 'CASHBACK', 'WITHDRAWAL'));
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category text;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source_transaction_id uuid REFERENCES transactions(id);
CREATE INDEX IF NOT EXISTS ix_transactions_account_created ON transactions(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_transactions_cashback_month
    ON transactions(account_id, created_at)
    WHERE transaction_type = 'CASHBACK';
CREATE TABLE IF NOT EXISTS account_cashback_settings (
    account_id uuid PRIMARY KEY REFERENCES accounts(id),
    extra_basis_points integer NOT NULL DEFAULT 0 CHECK (extra_basis_points BETWEEN 0 AND 3000),
    monthly_limit bigint NOT NULL DEFAULT 50000 CHECK (monthly_limit >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO account_cashback_settings(account_id)
SELECT id FROM accounts
ON CONFLICT (account_id) DO NOTHING;
CREATE TABLE IF NOT EXISTS account_requests (
    correlation_id uuid PRIMARY KEY,
    account_id uuid NOT NULL UNIQUE REFERENCES accounts(id),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    correlation_id uuid NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text
);
CREATE INDEX IF NOT EXISTS ix_outbox_events_pending
    ON outbox_events(next_attempt_at, created_at)
    WHERE published_at IS NULL;`)
	return err
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	var pending int64
	if err := a.db.QueryRow(r.Context(), "SELECT count(*) FROM outbox_events WHERE published_at IS NULL").Scan(&pending); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if err := a.redis.Ping(r.Context()).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "outboxPending": pending})
}

func (a *app) createAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.OwnerName) == "" {
		writeError(w, http.StatusBadRequest, "ownerName is required")
		return
	}
	corrID, err := parseOrNewUUID(req.CorrelationID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "correlationId must be a UUID")
		return
	}
	accountID := uuid.New()
	event := newEvent("AccountCreated", corrID, accountID, 0, 0, traceIDFromContext(r.Context()))
	tx, err := a.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer tx.Rollback(r.Context())
	_, err = tx.Exec(r.Context(), "INSERT INTO accounts(id, owner_name) VALUES($1, $2)", accountID, strings.TrimSpace(req.OwnerName))
	if err == nil {
		_, err = tx.Exec(r.Context(), "INSERT INTO account_cashback_settings(account_id) VALUES($1)", accountID)
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), "INSERT INTO account_requests(correlation_id, account_id) VALUES($1, $2)", corrID, accountID)
	}
	if err == nil {
		err = queueEvent(r.Context(), tx, event)
	}
	if err != nil {
		var existing uuid.UUID
		lookupErr := a.db.QueryRow(r.Context(), "SELECT account_id FROM account_requests WHERE correlation_id=$1", corrID).Scan(&existing)
		if lookupErr == nil {
			writeJSON(w, http.StatusOK, map[string]any{"accountId": existing, "balance": 0, "correlationId": corrID, "duplicate": true})
			return
		}
		writeError(w, http.StatusInternalServerError, "could not create account")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "could not commit account")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"accountId": accountID, "balance": 0, "correlationId": corrID, "eventId": event.EventID, "eventQueued": true})
}

func (a *app) topUp(w http.ResponseWriter, r *http.Request) {
	accountID, err := uuid.Parse(r.PathValue("accountId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "accountId must be a UUID")
		return
	}
	var req topUpRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer in minor currency units")
		return
	}
	req.Category = strings.ToLower(strings.TrimSpace(req.Category))
	if req.Category == "" {
		writeError(w, http.StatusBadRequest, "category is required")
		return
	}
	corrID, err := uuid.Parse(req.CorrelationID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "correlationId is required and must be a UUID")
		return
	}
	policy, err := a.loadCategoryPolicy(r.Context(), req.Category)
	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusBadRequest, "unknown category")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "cashback category cache unavailable")
		return
	}

	tx, err := a.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer tx.Rollback(r.Context())

	// Intentionally read for every top-up. This is a profiling hotspot and a future cache candidate.
	personal, err := loadPersonalCashback(r.Context(), tx, accountID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load personal cashback settings")
		return
	}
	monthlyCashback, err := loadCurrentMonthCashback(r.Context(), tx, accountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not calculate monthly cashback")
		return
	}
	cashback := calculateCashback(req.Amount, policy, personal, monthlyCashback)
	topUpTransactionID := uuid.New()
	cashbackCorrelationID := uuid.NewSHA1(corrID, []byte("cashback"))

	var balance int64
	err = tx.QueryRow(r.Context(), "UPDATE accounts SET balance=balance+$1, version=version+1 WHERE id=$2 RETURNING balance", req.Amount, accountID).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), `INSERT INTO transactions(id, account_id, transaction_type, amount, balance_after, correlation_id, category)
VALUES($1, $2, 'TOP_UP', $3, $4, $5, $6)`, topUpTransactionID, accountID, req.Amount, balance, corrID, policy.Code)
	}
	var topUpEvent bankEvent
	if err == nil {
		topUpEvent = newEvent("AccountToppedUp", corrID, accountID, req.Amount, balance, traceIDFromContext(r.Context()))
		topUpEvent.Category = policy.Code
		err = queueEvent(r.Context(), tx, topUpEvent)
	}
	var cashbackEvent bankEvent
	if err == nil && cashback > 0 {
		err = tx.QueryRow(r.Context(), "UPDATE accounts SET balance=balance+$1, version=version+1 WHERE id=$2 RETURNING balance", cashback, accountID).Scan(&balance)
	}
	if err == nil && cashback > 0 {
		_, err = tx.Exec(r.Context(), `INSERT INTO transactions(id, account_id, transaction_type, amount, balance_after, correlation_id, category, source_transaction_id)
VALUES($1, $2, 'CASHBACK', $3, $4, $5, $6, $7)`, uuid.New(), accountID, cashback, balance, cashbackCorrelationID, policy.Code, topUpTransactionID)
	}
	if err == nil && cashback > 0 {
		cashbackEvent = newEvent("CashbackGranted", cashbackCorrelationID, accountID, cashback, balance, traceIDFromContext(r.Context()))
		cashbackEvent.Category = policy.Code
		cashbackEvent.SourceAmount = req.Amount
		err = queueEvent(r.Context(), tx, cashbackEvent)
	}
	if err != nil {
		var existingAmount, existingBalance int64
		lookupErr := a.db.QueryRow(r.Context(), "SELECT amount, balance_after FROM transactions WHERE correlation_id=$1", corrID).Scan(&existingAmount, &existingBalance)
		if lookupErr == nil {
			writeJSON(w, http.StatusOK, map[string]any{"accountId": accountID, "amount": existingAmount, "balance": existingBalance, "correlationId": corrID, "duplicate": true})
			return
		}
		writeError(w, http.StatusInternalServerError, "could not top up account")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "could not commit top-up")
		return
	}

	response := map[string]any{
		"accountId": accountID, "amount": req.Amount, "category": policy.Code,
		"cashback": cashback, "balance": balance, "correlationId": corrID,
		"eventId": topUpEvent.EventID, "eventQueued": true,
	}
	if cashback > 0 {
		response["cashbackEventId"] = cashbackEvent.EventID
	}
	writeJSON(w, http.StatusOK, response)
}

func (a *app) withdraw(w http.ResponseWriter, r *http.Request) {
	accountID, err := uuid.Parse(r.PathValue("accountId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "accountId must be a UUID")
		return
	}

	var req withdrawalRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "amount must be a positive integer in minor currency units")
		return
	}
	corrID, err := uuid.Parse(req.CorrelationID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "correlationId is required and must be a UUID")
		return
	}

	if handled := a.writeExistingWithdrawal(w, r, accountID, corrID); handled {
		return
	}

	tx, err := a.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer tx.Rollback(r.Context())

	balance, result, err := withdrawOptimistically(r.Context(), tx, accountID, req.Amount)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not withdraw funds")
		return
	}
	switch result {
	case withdrawalAccountNotFound:
		writeError(w, http.StatusNotFound, "account not found")
		return
	case withdrawalInsufficientFunds:
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "insufficient funds", "accountId": accountID,
			"requestedAmount": req.Amount, "availableBalance": balance,
		})
		return
	case withdrawalConcurrentConflict:
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":     "account balance changed concurrently; retry the request",
			"accountId": accountID, "correlationId": corrID,
		})
		return
	}

	_, err = tx.Exec(r.Context(), `INSERT INTO transactions
(id, account_id, transaction_type, amount, balance_after, correlation_id)
VALUES($1, $2, 'WITHDRAWAL', $3, $4, $5)`, uuid.New(), accountID, req.Amount, balance, corrID)
	event := newEvent("AccountDebited", corrID, accountID, req.Amount, balance, traceIDFromContext(r.Context()))
	if err == nil {
		err = queueEvent(r.Context(), tx, event)
	}
	if err != nil {
		if handled := a.writeExistingWithdrawal(w, r, accountID, corrID); handled {
			return
		}
		writeError(w, http.StatusInternalServerError, "could not withdraw funds")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "could not commit withdrawal")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accountId": accountID, "amount": req.Amount, "balance": balance,
		"correlationId": corrID, "eventId": event.EventID, "eventQueued": true,
	})
}

func (a *app) writeExistingWithdrawal(w http.ResponseWriter, r *http.Request, accountID, corrID uuid.UUID) bool {
	var existingAccountID uuid.UUID
	var transactionType string
	var amount, balance int64
	err := a.db.QueryRow(r.Context(), `SELECT account_id, transaction_type, amount, balance_after
FROM transactions WHERE correlation_id=$1`, corrID).Scan(&existingAccountID, &transactionType, &amount, &balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		return false
	}
	if existingAccountID != accountID || transactionType != "WITHDRAWAL" {
		writeError(w, http.StatusConflict, "correlationId has already been used for another operation")
		return true
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accountId": accountID, "amount": amount, "balance": balance,
		"correlationId": corrID, "duplicate": true,
	})
	return true
}

func withdrawOptimistically(ctx context.Context, tx pgx.Tx, accountID uuid.UUID, amount int64) (int64, withdrawalResult, error) {
	for attempt := 0; attempt < maxWithdrawalAttempts; attempt++ {
		var balance, version int64
		err := tx.QueryRow(ctx, "SELECT balance, version FROM accounts WHERE id=$1", accountID).Scan(&balance, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, withdrawalAccountNotFound, nil
		}
		if err != nil {
			return 0, withdrawalConcurrentConflict, err
		}
		if balance < amount {
			return balance, withdrawalInsufficientFunds, nil
		}

		newBalance := balance - amount
		command, err := tx.Exec(ctx, `UPDATE accounts
SET balance=$1, version=version+1
WHERE id=$2 AND version=$3`, newBalance, accountID, version)
		if err != nil {
			return 0, withdrawalConcurrentConflict, err
		}
		if command.RowsAffected() == 1 {
			return newBalance, withdrawalApplied, nil
		}
	}
	return 0, withdrawalConcurrentConflict, nil
}

func seedCategoryCache(ctx context.Context, client *redis.Client) error {
	for _, policy := range defaultCategoryPolicies {
		payload, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		if err = client.SetNX(ctx, categoryCacheKey(policy.Code), payload, 0).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (a *app) loadCategoryPolicy(ctx context.Context, category string) (categoryPolicy, error) {
	payload, err := a.redis.Get(ctx, categoryCacheKey(category)).Bytes()
	if err != nil {
		return categoryPolicy{}, err
	}
	var policy categoryPolicy
	if err = json.Unmarshal(payload, &policy); err != nil {
		return categoryPolicy{}, fmt.Errorf("decode category %q: %w", category, err)
	}
	return policy, nil
}

func categoryCacheKey(category string) string {
	return "green-bank:cashback-category:" + category
}

func loadPersonalCashback(ctx context.Context, tx pgx.Tx, accountID uuid.UUID) (personalCashback, error) {
	var setting personalCashback
	err := tx.QueryRow(ctx, `SELECT extra_basis_points, monthly_limit
FROM account_cashback_settings WHERE account_id=$1`, accountID).Scan(&setting.ExtraBasisPoints, &setting.MonthlyLimit)
	return setting, err
}

func loadCurrentMonthCashback(ctx context.Context, tx pgx.Tx, accountID uuid.UUID) (int64, error) {
	var amount int64
	err := tx.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0)
FROM transactions
WHERE account_id=$1 AND transaction_type='CASHBACK'
  AND created_at >= date_trunc('month', now())`, accountID).Scan(&amount)
	return amount, err
}

func calculateCashback(amount int64, policy categoryPolicy, personal personalCashback, monthlyCashback int64) int64 {
	if amount < policy.MinimumAmount || personal.MonthlyLimit <= monthlyCashback {
		return 0
	}
	cashback := amount * (policy.CashbackBasisPoints + personal.ExtraBasisPoints) / 10_000
	if cashback > policy.MaximumPerOperation {
		cashback = policy.MaximumPerOperation
	}
	remainingMonthlyLimit := personal.MonthlyLimit - monthlyCashback
	if cashback > remainingMonthlyLimit {
		cashback = remainingMonthlyLimit
	}
	if cashback < 0 {
		return 0
	}
	return cashback
}

func newEvent(eventType string, correlationID, accountID uuid.UUID, amount, balance int64, traceID string) bankEvent {
	return bankEvent{EventID: uuid.NewString(), EventType: eventType, OccurredAt: time.Now().UTC(), CorrelationID: correlationID.String(), AccountID: accountID.String(), Amount: amount, Balance: balance, TraceID: traceID}
}

func queueEvent(ctx context.Context, tx pgx.Tx, event bankEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(event_id, event_type, aggregate_id, correlation_id, payload)
VALUES($1, $2, $3, $4, $5)`, event.EventID, event.EventType, event.AccountID, event.CorrelationID, payload)
	return err
}

func (a *app) runOutboxPublisher(ctx context.Context) {
	const idleDelay = 500 * time.Millisecond
	for {
		published, err := a.publishNext(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.log.Error("outbox publish cycle failed", "error", err)
		}
		if published {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idleDelay):
		}
	}
}

func (a *app) publishNext(ctx context.Context) (bool, error) {
	tx, err := a.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var eventID, eventType, aggregateID, correlationID string
	var payload []byte
	var attempts int
	err = tx.QueryRow(ctx, `
SELECT event_id::text, event_type, aggregate_id::text, correlation_id::text, payload, attempts
FROM outbox_events
WHERE published_at IS NULL AND next_attempt_at <= now()
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1`).Scan(&eventID, &eventType, &aggregateID, &correlationID, &payload, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = a.writer.WriteMessages(publishCtx, kafka.Message{
		Key:   []byte(aggregateID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "eventId", Value: []byte(eventID)},
			{Key: "eventType", Value: []byte(eventType)},
			{Key: "correlationId", Value: []byte(correlationID)},
		},
	})
	cancel()

	if err != nil {
		nextAttempts := attempts + 1
		delaySeconds := retryDelaySeconds(nextAttempts)
		_, updateErr := tx.Exec(ctx, `UPDATE outbox_events
SET attempts=$2, next_attempt_at=now()+($3 * interval '1 second'), last_error=$4
WHERE event_id=$1`, eventID, nextAttempts, delaySeconds, err.Error())
		if updateErr != nil {
			return false, updateErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return false, commitErr
		}
		a.log.Warn("outbox event publish deferred", "eventId", eventID, "attempts", nextAttempts, "retryInSeconds", delaySeconds, "error", err)
		return true, nil
	}

	_, err = tx.Exec(ctx, `UPDATE outbox_events
SET published_at=now(), attempts=attempts+1, last_error=NULL
WHERE event_id=$1`, eventID)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	a.log.Info("outbox event published", "eventId", eventID, "eventType", eventType, "correlationId", correlationID, "accountId", aggregateID)
	return true, nil
}

func retryDelaySeconds(attempt int) int {
	if attempt < 1 {
		return 1
	}
	if attempt > 6 {
		return 60
	}
	return 1 << (attempt - 1)
}

func parseOrNewUUID(value string) (uuid.UUID, error) {
	if strings.TrimSpace(value) == "" {
		return uuid.New(), nil
	}
	return uuid.Parse(value)
}

func requestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
		if traceID == "" || len(traceID) > 128 {
			traceID = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), traceIDContextKey{}, traceID)
		r = r.WithContext(ctx)
		w.Header().Set("X-Trace-ID", traceID)
		recorder := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()
		logger.Info("HTTP_REQUEST_STARTED", "traceId", traceID, "method", r.Method, "path", r.URL.Path)
		defer func() {
			duration := time.Since(started)
			if recovered := recover(); recovered != nil {
				logger.Error("HTTP_REQUEST_PANIC", "traceId", traceID, "method", r.Method, "path", r.URL.Path, "durationMs", duration.Milliseconds(), "panic", recovered)
				if !recorder.wroteHeader {
					http.Error(recorder, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
				return
			}
			logger.Info("HTTP_REQUEST_COMPLETED", "traceId", traceID, "method", r.Method, "path", r.URL.Path, "status", recorder.status, "durationMs", duration.Milliseconds(), "bytes", recorder.bytes)
		}()
		next.ServeHTTP(recorder, r)
	})
}

type traceIDContextKey struct{}

func traceIDFromContext(ctx context.Context) string {
	traceID, _ := ctx.Value(traceIDContextKey{}).(string)
	return traceID
}

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
