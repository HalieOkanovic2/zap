package zapcore

import (
	"bufio"
	"sync"
	"time"

	"go.uber.org/multierr"
)

const (
	_defaultBufferSize   = 256 * 1024 // 256 kB
	_defaultFlushInterval = 30 * time.Second
)

// A BufferedWriteSyncer is a WriteSyncer that double-buffers writes.
//
// A BufferedWriteSyncer is safe for concurrent use.
type BufferedWriteSyncer struct {
	// WS is the WriteSyncer wrapped by BufferedWriteSyncer.
	WS WriteSyncer

	// Size specifies the maximum amount of data the writer will buffer
	// before flushing.
	// Defaults to 256 kB.
	Size int

	// FlushInterval specifies how often the writer should flush its buffer.
	// Defaults to 30 seconds.
	FlushInterval time.Duration

	// Clock, if specified, provides the time source.
	// Defaults to system time.
	Clock Clock

	mu          sync.Mutex
	initialized bool
	writer      *bufio.Writer
	ticker      *time.Ticker
	stop        chan struct{}
	done        chan struct{}
}

func (s *BufferedWriteSyncer) initialize() {
	s.initialized = true

	if s.Size == 0 {
		s.Size = _defaultBufferSize
	}

	if s.FlushInterval == 0 {
		s.FlushInterval = _defaultFlushInterval
	}

	if s.Clock == nil {
		s.Clock = DefaultClock
	}

	s.writer = bufio.NewWriterSize(s.WS, s.Size)
	s.ticker = s.Clock.NewTicker(s.FlushInterval)
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go s.flushLoop()
}

// Write writes to the underlying bufio.Writer.
func (s *BufferedWriteSyncer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		s.initialize()
	}

	if len(p) > s.writer.Available() && s.writer.Buffered() > 0 {
		if err := s.writer.Flush(); err != nil {
			return 0, err
		}
	}
	return s.writer.Write(p)
}

// Sync flushes buffered data and syncs the underlying WriteSyncer.
func (s *BufferedWriteSyncer) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return s.WS.Sync()
	}

	err := s.writer.Flush()
	err = multierr.Append(err, s.WS.Sync())
	return err
}

// Stop closes the buffer, flushing any remaining data.
func (s *BufferedWriteSyncer) Stop() error {
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return nil
	}

	s.ticker.Stop()
	close(s.stop)
	s.mu.Unlock()

	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.writer.Flush()
	err = multierr.Append(err, s.WS.Sync())
	return err
}

func (s *BufferedWriteSyncer) flushLoop() {
	defer close(s.done)

	for {
		select {
		case <-s.ticker.C:
			s.mu.Lock()
			_ = s.writer.Flush() // ignore error in background flush
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}
