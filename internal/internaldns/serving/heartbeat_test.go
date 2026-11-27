package serving

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartHeartbeatsOnlyWhileReconcileProgresses(t *testing.T) {
	dir := t.TempDir()
	renderConfig, snapshot := renderFixture(t)
	renderConfig.Role = RenderNode
	renderConfig.NodeDNSDist.Path = filepath.Join(dir, "runtime", "node-dnsdist.conf")
	renderConfig.NodeCoreDNS.Path = filepath.Join(dir, "runtime", "node-coredns.json")
	renderConfig.ClusterDNSDist = DNSDistRenderConfig{}
	renderConfig.ClusterCoreDNS = CoreDNSRenderConfig{}
	snapshot.Bindings[0].Zones = nil
	var beats atomic.Int64
	agent, err := NewAgent(Config{
		Region: "east", Shard: "s1", MemberID: "node-0", StateDir: filepath.Join(dir, "state"), Mode: ModeResolver,
		Render: renderConfig, Transaction: TransactionConfig{StageDir: filepath.Join(dir, "stage"), FailClosed: []Command{{Path: "gate"}}},
		UnsafeAllowNoFailClosed: true, UnsafeAllowNoDNSVerify: true,
		ReconcileInterval: 10 * time.Millisecond,
	}, Dependencies{Runner: &recordingRunner{}, Heartbeat: func() { beats.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	agent.state.Snapshot = &snapshot
	agent.state.BindingFences, _ = advanceBindingFences(nil, snapshot)
	agent.state.ServingFence = fence{Epoch: 1, Revision: 1}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- agent.Start(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	waitForBeats := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for beats.Load() < want {
			if time.Now().After(deadline) {
				t.Fatalf("reconcile loop recorded %d heartbeats, want at least %d", beats.Load(), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitForBeats(3)

	agent.mu.Lock()
	time.Sleep(100 * time.Millisecond)
	stalled := beats.Load()
	time.Sleep(200 * time.Millisecond)
	if got := beats.Load(); got != stalled {
		agent.mu.Unlock()
		t.Fatalf("stalled reconcile loop recorded heartbeats: %d then %d", stalled, got)
	}
	agent.mu.Unlock()

	waitForBeats(stalled + 3)
}
