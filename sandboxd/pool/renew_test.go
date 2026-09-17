package pool

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestRenewPreservesIdentityAndPersists(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	originalID, token, vm, previous := sb.ID, sb.Token, sb.VMName, sb.Deadline
	deadline, err := m.Renew(t.Context(), sb.ID, sb.Token, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !deadline.After(previous) || sb.ID != originalID || sb.Token != token || sb.VMName != vm {
		t.Fatal("renewal changed identity or failed to extend")
	}
	loaded, err := newClaimStore(m.dataDir).load()
	if err != nil {
		t.Fatal(err)
	}
	if !loaded[sb.ID].Deadline.Equal(deadline) {
		t.Fatal("renewal not durable")
	}
	shorter, err := m.Renew(t.Context(), sb.ID, sb.Token, time.Second)
	if err != nil || !shorter.Equal(deadline) {
		t.Fatal("renewal shortened lease")
	}
}

func TestRenewRejectsWrongTokenExpiredAndRemoved(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	if _, err := m.Renew(t.Context(), sb.ID, "wrong", time.Hour); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatal(err)
	}
	m.mu.Lock()
	sb.Deadline = time.Now().Add(-time.Second)
	m.mu.Unlock()
	if _, err := m.Renew(t.Context(), sb.ID, sb.Token, time.Hour); !errors.Is(err, ErrLeaseExpired) {
		t.Fatal(err)
	}
	m.reapOnce(t.Context())
	if _, err := m.Renew(t.Context(), sb.ID, sb.Token, time.Hour); !errors.Is(err, ErrUnknownSandbox) {
		t.Fatal("resurrected reaped claim")
	}
}

func TestRenewPersistenceFailureRollsBack(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	before := sb.Deadline
	m.store.path = t.TempDir() // rename cannot replace a directory
	if _, err := m.Renew(t.Context(), sb.ID, sb.Token, time.Hour); err == nil {
		t.Fatal("acknowledged failed persistence")
	}
	if !sb.Deadline.Equal(before) {
		t.Fatal("memory deadline diverged from disk")
	}
}

func TestRenewConcurrentWithRelease(t *testing.T) {
	m := newTestManager(t, newFakeEngine())
	sb := mustClaim(t, m, testKey)
	id, token := sb.ID, sb.Token
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, err := m.Renew(t.Context(), id, token, time.Hour)
			if err != nil && !errors.Is(err, ErrUnknownSandbox) {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		if err := m.Release(t.Context(), id, token); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	loaded, err := newClaimStore(m.dataDir).load()
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if loaded[id] != nil {
		t.Fatal("released claim resurrected on disk")
	}
}
