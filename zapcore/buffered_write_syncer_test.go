package zapcore

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type safeBuffer struct {
	mu sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) Sync() error {
	return nil
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestBufferedWriteSyncerConcurrency(t *testing.T) {
	memBuf := &safeBuffer{}
	ws := &BufferedWriteSyncer{
		WS:            memBuf,
		Size:          4096,
		FlushInterval: time.Millisecond * 10,
	}
	defer ws.Stop()

	var wg sync.WaitGroup
	const (
		numGoroutines = 100
		numWrites     = 10
	)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numWrites; j++ {
				_, err := ws.Write([]byte("log\n"))
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()
	require.NoError(t, ws.Sync())

	// Count lines in memBuf
	lines := bytes.Split(bytes.TrimSpace([]byte(memBuf.String())), []byte("\n"))
	assert.Equal(t, numGoroutines*numWrites, len(lines))
}
