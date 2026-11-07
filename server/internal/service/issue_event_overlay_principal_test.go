package service

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// Unattended Task attribution must not silently grant a different member's
// Connected Apps to the workspace-configured actor. This is a fail-closed
// credential-scoping check, not a change to original Issue ownership.
func TestNativeEventOverlayPrincipalAllowed(t *testing.T) {
	creator := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	actor := pgtype.UUID{Bytes: [16]byte{2}, Valid: true}
	tests := []struct {
		name string
		originator pgtype.UUID
		actor pgtype.UUID
		want bool
	}{
		{"same authorized principal", creator, creator, true},
		{"different issue creator and configured actor", creator, actor, false},
		{"unknown originator", pgtype.UUID{}, creator, false},
		{"missing configured actor", creator, pgtype.UUID{}, false},
		{"both absent", pgtype.UUID{}, pgtype.UUID{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nativeEventOverlayPrincipalAllowed(tt.originator, tt.actor); got != tt.want {
				t.Fatalf("connected-app principal gate = %v, want %v", got, tt.want)
			}
		})
	}
}
