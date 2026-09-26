package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type app struct {
	db     *pgxpool.Pool
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
}

type createAccountRequest struct {
	OwnerName     string `json:"ownerName"`
	CorrelationID string `json:"correlationId"`
}

type topUpRequest struct {
	Amount        int64  `json:"amount"`
	CorrelationID string `json:"correlationId"`
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

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := env("KAFKA_TOPIC", "bank.account.events")
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()

	a := &app{db: db, writer: writer, topic: topic, log: logger}
	go a.runOutboxPublisher(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("POST /accounts", a.createAccount)
	mux.HandleFunc("POST /accounts/{accountId}/top-ups", a.topUp)

	server := &http.Server{Addr: ":8080", Handler: requestLog(logger, mux), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("GreenBankService started", "address", server.Addr, "topic", topic)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("http server failed", "error", serveErr)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
CREATE TABLE IF NOT EXISTS accounts (
    id uuid PRIMARY KEY,
    owner_name text NOT NULL,
    balance bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS transactions (
    id uuid PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES accounts(id),
    transaction_type text NOT NULL CHECK (transaction_type IN ('TOP_UP')),
    amount bigint NOT NULL CHECK (amount > 0),
    balance_after bigint NOT NULL CHECK (balance_after >= 0),
    correlation_id uuid NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_transactions_account_created ON transactions(account_id, created_at DESC);
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
	event := newEvent("AccountCreated", corrID, accountID, 0, 0)
	tx, err := a.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer tx.Rollback(r.Context())
	_, err = tx.Exec(r.Context(), "INSERT INTO accounts(id, owner_name) VALUES($1, $2)", accountID, strings.TrimSpace(req.OwnerName))
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
	corrID, err := uuid.Parse(req.CorrelationID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "correlationId is required and must be a UUID")
		return
	}

	tx, err := a.db.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer tx.Rollback(r.Context())
	var balance int64
	err = tx.QueryRow(r.Context(), "UPDATE accounts SET balance=balance+$1 WHERE id=$2 RETURNING balance", req.Amount, accountID).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}
	if err == nil {
		_, err = tx.Exec(r.Context(), `INSERT INTO transactions(id, account_id, transaction_type, amount, balance_after, correlation_id)
VALUES($1, $2, 'TOP_UP', $3, $4, $5)`, uuid.New(), accountID, req.Amount, balance, corrID)
	}
	var event bankEvent
	if err == nil {
		event = newEvent("AccountToppedUp", corrID, accountID, req.Amount, balance)
		err = queueEvent(r.Context(), tx, event)
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

	writeJSON(w, http.StatusOK, map[string]any{"accountId": accountID, "amount": req.Amount, "balance": balance, "correlationId": corrID, "eventId": event.EventID, "eventQueued": true})
}

func newEvent(eventType string, correlationID, accountID uuid.UUID, amount, balance int64) bankEvent {
	return bankEvent{EventID: uuid.NewString(), EventType: eventType, OccurredAt: time.Now().UTC(), CorrelationID: correlationID.String(), AccountID: accountID.String(), Amount: amount, Balance: balance}
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
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "durationMs", time.Since(started).Milliseconds())
	})
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
