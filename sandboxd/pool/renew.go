package pool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrLeaseExpired = errors.New("sandbox lease expired")

// Renew extends one live lease without waking the guest or changing its identity.
// Authorization, expiry/reaper exclusion and durable acknowledgement are atomic.
func (m *Manager) Renew(_ context.Context, id, token string, ttl time.Duration) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb, ok := m.authed(id, token)
	if !ok {
		return time.Time{}, ErrUnknownSandbox
	}
	now := time.Now()
	if sb.ArchiveCk != "" || !sb.Deadline.After(now) {
		return time.Time{}, ErrLeaseExpired
	}
	previous := sb.Deadline
	deadline := now.Add(clampTTL(ttl))
	if deadline.After(previous) {
		sb.Deadline = deadline
	}
	if err := m.store.commit(m.store.snapshot(m.claimed)); err != nil {
		sb.Deadline = previous
		return time.Time{}, fmt.Errorf("persist sandbox renewal: %w", err)
	}
	return sb.Deadline, nil
}
