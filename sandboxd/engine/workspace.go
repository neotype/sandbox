package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var workspaceID = regexp.MustCompile(`^[a-f0-9]{64}$`)

type workspaceRecord struct {
	VMName      string `json:"vm_name,omitempty"`
	Socket      string `json:"socket,omitempty"`
	Initialized bool   `json:"initialized"`
}

// EnableWorkspaces is opt-in and single-node. The holder journal contains no
// bearer tokens or user identities. Disks are operator-owned, never VM garbage.
func (e *Engine) EnableWorkspaces(dir string, size int64) error {
	if dir == "" {
		return nil
	}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return fmt.Errorf("workspace_dir must be an absolute clean path")
	}
	if size == 0 {
		size = 8 << 30
	}
	if size < 1<<30 || size > 64<<30 {
		return fmt.Errorf("workspace disk must be 1–64 GiB")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unsafe workspace directory")
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return fmt.Errorf("workspace directory already has a writer: %w", err)
	}
	e.workspaceLock = lock
	e.workspaceDir = dir
	e.workspaceSize = size
	e.workspaces = map[string]workspaceRecord{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if !workspaceID.MatchString(id) {
			return fmt.Errorf("invalid workspace binding filename")
		}
		p := filepath.Join(dir, entry.Name())
		if err := regularFile(p); err != nil {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var rec workspaceRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return fmt.Errorf("invalid workspace binding: %w", err)
		}
		if (rec.VMName == "") != (rec.Socket == "") {
			return fmt.Errorf("incomplete workspace holder binding")
		}
		e.workspaces[id] = rec
	}
	return nil
}

func regularFile(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("workspace backing must be a regular file")
	}
	return nil
}

// Persist before acknowledging: write, fsync, rename, fsync directory.
func (e *Engine) saveWorkspace(id string, rec workspaceRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(e.workspaceDir, ".binding-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp, filepath.Join(e.workspaceDir, id+".json")); err != nil {
		return err
	}
	d, err := os.Open(e.workspaceDir)
	if err != nil {
		return err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return err
	}
	e.workspaces[id] = rec
	return nil
}

func (e *Engine) PrepareWorkspace(ctx context.Context, id string, create bool) error {
	e.workspaceMu.Lock()
	defer e.workspaceMu.Unlock()
	if e.workspaceDir == "" || !workspaceID.MatchString(id) {
		return fmt.Errorf("persistent workspace unavailable or invalid identity")
	}
	disk := filepath.Join(e.workspaceDir, id+".img")
	if _, ok := e.workspaces[id]; ok {
		return regularFile(disk)
	}
	if !create {
		return fmt.Errorf("workspace backing missing; refusing empty replacement")
	}
	// Never format an existing image, even if its journal was lost.
	if _, err := os.Lstat(disk); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unregistered workspace disk; refusing overwrite")
	}
	if len(e.workspaces) >= 32 {
		return fmt.Errorf("workspace capacity reached; operator cleanup required")
	}
	f, err := os.CreateTemp(e.workspaceDir, ".image-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	err = f.Chmod(0600)
	if err == nil {
		err = f.Truncate(e.workspaceSize)
	}
	f.Close()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", tmp).CombinedOutput(); err != nil {
		return fmt.Errorf("format new workspace: %w (%s)", err, out)
	}
	f, err = os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, disk); err != nil {
		return err
	}
	return e.saveWorkspace(id, workspaceRecord{})
}

func (e *Engine) AttachWorkspace(ctx context.Context, id, vm, socket string) error {
	e.workspaceMu.Lock()
	defer e.workspaceMu.Unlock()
	rec, ok := e.workspaces[id]
	if !ok {
		return fmt.Errorf("workspace not prepared")
	}
	disk := filepath.Join(e.workspaceDir, id+".img")
	if err := regularFile(disk); err != nil {
		return err
	}
	if rec.VMName != "" {
		vms, err := e.List(ctx)
		if err != nil {
			return err
		}
		for _, old := range vms {
			if old.Config.Name == rec.VMName {
				return fmt.Errorf("workspace still attached; refusing concurrent writer")
			}
		}
	}
	rec.VMName = vm
	rec.Socket = socket
	// Journal BEFORE attach: even an interrupted attach reserves its disk.
	if err := e.saveWorkspace(id, rec); err != nil {
		return err
	}
	if _, err := e.run(ctx, "vm", "disk", "attach", vm, "--path", disk, "--name", "pigeon-workspace", "--directio", "on"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Dynamic arguments are a validated hex identity and a fixed mount; there is
	// no caller shell text. Match kernel serial, never guess a /dev/vdX ordinal.
	command := `set -eu; dev=''; for i in $(seq 1 100); do for s in /sys/block/*/serial; do if [ -f "$s" ] && [ "$(cat "$s")" = pigeon-workspace ]; then b=${s#/sys/block/}; b=${b%/serial}; if [ -b "/dev/$b" ]; then dev="/dev/$b"; break; fi; fi; done; [ -z "$dev" ] || break; sleep 0.1; done; [ -n "$dev" ]; mkdir -p /workspace; mount -t ext4 -o rw -- "$dev" /workspace; `
	if !rec.Initialized {
		command += `printf '%s\n' ` + id + ` > /workspace/.pigeon-workspace-id; sync; `
	}
	command += `test "$(cat /workspace/.pigeon-workspace-id)" = ` + id
	if err := e.silkdExec(ctx, socket, "sh", "-c", command); err != nil {
		return err
	}
	rec.Initialized = true
	return e.saveWorkspace(id, rec)
}

func (e *Engine) VerifyWorkspace(ctx context.Context, id, socket string) error {
	if !workspaceID.MatchString(id) {
		return fmt.Errorf("invalid workspace identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.silkdExec(ctx, socket, "sh", "-c", `mountpoint -q /workspace && test "$(cat /workspace/.pigeon-workspace-id)" = `+id)
}

// TTL teardown and explicit release both flow through Remove. Flush/unmount is
// bounded; after a crash ext4 replays its journal on the next RW mount. A failed
// VM removal NEVER clears the holder, so a second guest cannot write the disk.
func (e *Engine) removeWorkspaceVM(ctx context.Context, name string) error {
	e.workspaceMu.Lock()
	defer e.workspaceMu.Unlock()
	var id string
	var rec workspaceRecord
	for key, r := range e.workspaces {
		if r.VMName == name {
			id = key
			rec = r
			break
		}
	}
	if id != "" {
		flush, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = e.silkdExec(flush, rec.Socket, "sh", "-c", `sync; umount /workspace`)
		cancel()
	}
	if _, err := e.run(ctx, "vm", "rm", "--force", name); err != nil {
		return err
	}
	if id != "" {
		vms, err := e.List(ctx)
		if err != nil {
			return err
		}
		for _, vm := range vms {
			if vm.Config.Name == name {
				return fmt.Errorf("workspace VM removal not confirmed")
			}
		}
		rec.VMName = ""
		rec.Socket = ""
		return e.saveWorkspace(id, rec)
	}
	return nil
}
