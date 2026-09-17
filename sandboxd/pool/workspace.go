package pool

import (
	"context"
	"fmt"
	"github.com/cocoonstack/sandbox/sandboxd/types"
	"time"
)

type workspaceEngine interface {
	PrepareWorkspace(context.Context, string, bool) error
	AttachWorkspace(context.Context, string, string, string) error
	VerifyWorkspace(context.Context, string, string) error
}

// Root-only idempotent admission. Disk identity is independent of compute leases.
// The engine's durable holder journal excludes concurrent writers, including
// reaper teardown and claims left behind by an interrupted provisioning request.
func (m *Manager) ClaimWorkspace(ctx context.Context, req types.ClaimRequest) (*types.Sandbox, error) {
	m.workspaceMu.Lock()
	defer m.workspaceMu.Unlock()
	eng, ok := m.eng.(workspaceEngine)
	if !ok || req.Workspace == nil {
		return nil, fmt.Errorf("%w: workspaces unavailable", ErrBadKey)
	}
	key := req.Key()
	if err := m.validate(key); err != nil {
		return nil, err
	}
	id := req.Workspace.ID
	if err := eng.PrepareWorkspace(ctx, id, req.Workspace.Create); err != nil {
		return nil, err
	}
	// Copy under the manager lock: the reaper and renewal mutate the live record.
	m.mu.Lock()
	var prior *types.Sandbox
	for _, sb := range m.claimed {
		if sb.WorkspaceID == id {
			prior = &types.Sandbox{ID: sb.ID, Token: sb.Token, Key: sb.Key, ClaimRef: sb.ClaimRef, VsockSocket: sb.VsockSocket, Deadline: sb.Deadline}
			break
		}
	}
	m.mu.Unlock()
	if prior != nil {
		if prior.Key != key || prior.ClaimRef != req.ClaimRef {
			return nil, fmt.Errorf("%w: workspace binding mismatch", ErrBadKey)
		}
		if prior.Deadline.After(time.Now()) {
			deadline, err := m.Renew(ctx, prior.ID, prior.Token, req.TTL())
			if err != nil {
				return nil, err
			}
			if err := eng.VerifyWorkspace(ctx, id, prior.VsockSocket); err != nil {
				return nil, err
			}
			prior.Deadline = deadline
			prior.WorkspaceID = id
			return prior, nil
		}
		// TTL only disposes compute; Remove flushes the disk and clears the holder
		// only after confirmed VM deletion. A failed removal blocks reattachment.
		if err := m.ReleaseOperator(ctx, prior.ID); err != nil {
			return nil, err
		}
	}
	if err := m.overQuota(1, ""); err != nil {
		return nil, err
	}
	golden, release, err := m.resolveGolden(ctx, key)
	if err != nil {
		return nil, err
	}
	defer release()
	sb, err := m.provision(ctx, key, golden)
	if err != nil {
		return nil, err
	}
	if err := eng.AttachWorkspace(ctx, id, sb.VMName, sb.VsockSocket); err != nil {
		m.destroy(context.WithoutCancel(ctx), sb.VMName)
		return nil, err
	}
	sb.WorkspaceID = id
	sb.ClaimRef = req.ClaimRef
	return m.finalize(ctx, sb, req.TTL())
}
