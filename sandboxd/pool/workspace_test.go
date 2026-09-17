package pool

import (
	"context"
	"fmt"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"strings"
	"testing"
	"time"
)

type workspaceFake struct {
	*fakeEngine
	disk        string
	attached    []string
	verifyError bool
}

func (f *workspaceFake) PrepareWorkspace(_ context.Context, id string, create bool) error {
	if f.disk == "" && create {
		f.disk = id
	}
	if f.disk != id {
		return fmt.Errorf("missing disk")
	}
	return nil
}
func (f *workspaceFake) AttachWorkspace(_ context.Context, id, vm, sock string) error {
	f.attached = append(f.attached, id)
	return nil
}
func (f *workspaceFake) VerifyWorkspace(_ context.Context, id, sock string) error {
	if f.verifyError {
		return fmt.Errorf("mount missing")
	}
	return nil
}
func TestWorkspaceOutlivesComputeAndRestarts(t *testing.T) {
	f := &workspaceFake{fakeEngine: newFakeEngine()}
	m := newTestManager(t, f.fakeEngine)
	m.eng = f
	id := strings.Repeat("a", 64)
	req := types.ClaimRequest{Template: "rt:24.04", ClaimRef: "opaque-owner", Workspace: &types.WorkspaceRequest{ID: id, Create: true}}
	sb, err := m.ClaimWorkspace(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := newClaimStore(m.dataDir).load()
	if err != nil || loaded[sb.ID].WorkspaceID != id {
		t.Fatal("binding not persisted")
	}
	same, err := m.ClaimWorkspace(t.Context(), req)
	if err != nil || same.ID != sb.ID || len(f.attached) != 1 {
		t.Fatal("claim was not idempotent")
	}
	restart := newTestManagerAt(t, f.fakeEngine, m.dataDir)
	restart.eng = f
	if err := restart.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	same, err = restart.ClaimWorkspace(t.Context(), req)
	if err != nil || same.ID != sb.ID {
		t.Fatal("failed to recover binding after restart")
	}
	restart.mu.Lock()
	restart.claimed[sb.ID].Deadline = time.Now().Add(-time.Second)
	restart.mu.Unlock()
	next, err := restart.ClaimWorkspace(t.Context(), req)
	if err != nil || next.ID == sb.ID || next.WorkspaceID != id || len(f.attached) != 2 {
		t.Fatalf("compute replacement lost disk: %v", err)
	}
	f.verifyError = true
	if _, err := restart.ClaimWorkspace(t.Context(), req); err == nil {
		t.Fatal("returned receipt for unverified mount")
	}
	req.Workspace = &types.WorkspaceRequest{ID: strings.Repeat("b", 64)}
	if _, err := restart.ClaimWorkspace(t.Context(), req); err == nil {
		t.Fatal("replaced missing workspace with empty disk")
	}
}
