package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkspaceSafetyAndDurableBinding(t *testing.T) {
	dir := t.TempDir()
	id := strings.Repeat("a", 64)
	e := New("false", "", "", false, "")
	if err := e.EnableWorkspaces(dir, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareWorkspace(t.Context(), "../escape", true); err == nil {
		t.Fatal("accepted unsafe identity")
	}
	if err := e.PrepareWorkspace(t.Context(), id, false); err == nil {
		t.Fatal("created missing recovery disk")
	}
	disk := filepath.Join(dir, id+".img")
	if err := os.WriteFile(disk, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareWorkspace(t.Context(), id, true); err == nil {
		t.Fatal("formatted unregistered disk")
	}
	rec := workspaceRecord{VMName: "sbx-held", Socket: "/missing-socket", Initialized: true}
	if err := e.saveWorkspace(id, rec); err != nil {
		t.Fatal(err)
	}
	e.workspaceLock.Close()
	restarted := New("false", "", "", false, "")
	if err := restarted.EnableWorkspaces(dir, 0); err != nil {
		t.Fatal(err)
	}
	if restarted.workspaces[id] != rec {
		t.Fatal("lost holder across restart")
	}
	if err := restarted.PrepareWorkspace(t.Context(), id, false); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Remove(t.Context(), rec.VMName); err == nil {
		t.Fatal("ignored VM remove failure")
	}
	if restarted.workspaces[id].VMName != rec.VMName {
		t.Fatal("released holder after failed deletion")
	}
	if err := os.Remove(disk); err != nil {
		t.Fatal(err)
	}
	if err := restarted.PrepareWorkspace(t.Context(), id, true); err == nil {
		t.Fatal("recreated known missing disk")
	}
}

func TestWorkspaceRejectsSymlinkBackingAndCorruptJournal(t *testing.T) {
	dir := t.TempDir()
	id := strings.Repeat("b", 64)
	e := New("false", "", "", false, "")
	if err := e.EnableWorkspaces(dir, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.saveWorkspace(id, workspaceRecord{Initialized: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", filepath.Join(dir, id+".img")); err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareWorkspace(t.Context(), id, false); err == nil {
		t.Fatal("accepted symlink backing")
	}
	if err := os.WriteFile(filepath.Join(dir, id+".json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	e.workspaceLock.Close()
	e = New("false", "", "", false, "")
	if err := e.EnableWorkspaces(dir, 0); err == nil {
		t.Fatal("ignored corrupt journal")
	}
}
