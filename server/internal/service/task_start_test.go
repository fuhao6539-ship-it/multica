package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type startReplayDB struct {
	mockDBTX
	startErr error
	calls    int
}

// A normal Task has no event Issue to pre-lock. Model PostgreSQL SELECT :many
// as a successful empty result, not ErrNoRows (which pgx Query does not emit).
func (m *startReplayDB) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "LockNativeOracleEventIssueBeforeTaskStart") {
		return &sliceRows{}, nil
	}
	return nil, pgx.ErrNoRows
}

func (m *startReplayDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	m.calls++
	return &mockRow{err: m.startErr}
}

func TestStartTaskLegacyDoesNotAcknowledgeReplayOrDatabaseFailure(t *testing.T) {
	for _, want := range []error{pgx.ErrNoRows, errors.New("database unavailable")} {
		store := &startReplayDB{startErr: want}
		svc := &TaskService{Queries: db.New(store)}
		got, err := svc.StartTask(context.Background(), testUUID(1))
		if got != nil || !errors.Is(err, want) || store.calls != 1 {
			t.Fatalf("legacy start: task=%v err=%v queries=%d", got, err, store.calls)
		}
	}
}
