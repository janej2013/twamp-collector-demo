// Package receiver owns the UDP socket and the read loops. A reader
// goroutine does exactly three things: timestamp the arrival, read into
// a pooled buffer, and hand the parsed result to the pipeline — anything
// slower lives downstream, behind the pipeline's drop-not-block boundary.
package receiver

import (
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
)

// Enqueuer is the receiver's consumer-side view of the pipeline —
// declared here, where it is used, per Go interface convention.
type Enqueuer interface {
	Enqueue(pipeline.Measurement) bool
}

// bufSize covers a TWAMP-Test packet (15 bytes) plus any sane padding
// without making the pool hold megabytes.
const bufSize = 2048

// Config for the receiver. Zero values fall back to defaults.
type Config struct {
	Addr    string // UDP listen address, e.g. ":8620"
	Readers int    // concurrent read goroutines (default 2)
	Batch   int    // datagrams per ReadBatch syscall (default 32)

	// ForceReadFrom selects the portable one-datagram-per-syscall path
	// even on Linux. Exists so both paths stay tested and comparable.
	ForceReadFrom bool
}

type Receiver struct {
	conn    *net.UDPConn
	pc      *ipv4.PacketConn
	enq     Enqueuer
	readers int
	batch   int
	useFrom bool

	// pool stores *[]byte rather than []byte: a slice stored directly in
	// an interface value escapes and allocates on every Put (staticcheck
	// SA6002); the pointer form does not.
	pool sync.Pool
	wg   sync.WaitGroup

	received  atomic.Uint64
	parseErrs atomic.Uint64
	readErrs  atomic.Uint64
}

// New binds the UDP socket so callers see address errors immediately;
// no goroutines run until Start.
func New(cfg Config, enq Enqueuer) (*Receiver, error) {
	if cfg.Readers <= 0 {
		cfg.Readers = 2
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 32
	}
	addr, err := net.ResolveUDPAddr("udp", cfg.Addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	r := &Receiver{
		conn:    conn,
		pc:      ipv4.NewPacketConn(conn),
		enq:     enq,
		readers: cfg.Readers,
		batch:   cfg.Batch,
		useFrom: cfg.ForceReadFrom || runtime.GOOS != "linux",
	}
	r.pool.New = func() any {
		b := make([]byte, bufSize)
		return &b
	}
	return r, nil
}

// Start launches the reader goroutines and returns immediately.
func (r *Receiver) Start() {
	for i := 0; i < r.readers; i++ {
		r.wg.Add(1)
		if r.useFrom {
			go r.readFromLoop()
		} else {
			go r.readBatchLoop()
		}
	}
}

// Close stops intake: closing the socket unblocks every reader with
// net.ErrClosed, and Close returns once they have all exited — after
// this, no Enqueue is in flight, so the pipeline may be closed safely.
func (r *Receiver) Close() error {
	err := r.conn.Close()
	r.wg.Wait()
	return err
}

// LocalAddr reports the bound address (useful with ":0" in tests).
func (r *Receiver) LocalAddr() net.Addr { return r.conn.LocalAddr() }

// Received, ParseErrors and ReadErrors expose counters for metrics export.
func (r *Receiver) Received() uint64    { return r.received.Load() }
func (r *Receiver) ParseErrors() uint64 { return r.parseErrs.Load() }
func (r *Receiver) ReadErrors() uint64  { return r.readErrs.Load() }

// readBatchLoop drains up to Batch datagrams per recvmmsg(2) syscall via
// x/net's ReadBatch. At 50k pps the portable path pays 50k syscalls/s;
// with Batch=32 this path pays ~1.6k — syscall overhead is the receive
// bottleneck, so this is the single biggest win available in userspace.
func (r *Receiver) readBatchLoop() {
	defer r.wg.Done()
	// The loop owns its buffers for its whole lifetime, so it takes them
	// from the pool once instead of churning Get/Put per call; the pool
	// still recycles them across reader restarts and both read paths.
	msgs := make([]ipv4.Message, r.batch)
	bufs := make([]*[]byte, r.batch)
	for i := range msgs {
		bufs[i] = r.pool.Get().(*[]byte)
		msgs[i].Buffers = [][]byte{*bufs[i]}
	}
	defer func() {
		for _, b := range bufs {
			r.pool.Put(b)
		}
	}()
	for {
		n, err := r.pc.ReadBatch(msgs, 0)
		// One timestamp per syscall return, as early as possible: every
		// instruction between arrival and time.Now() inflates measured
		// delay. Sharing it across the batch is the accepted error;
		// per-packet kernel timestamps (SO_TIMESTAMPING) would remove it.
		rx := time.Now()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			r.readErrs.Add(1)
			continue
		}
		for i := 0; i < n; i++ {
			r.handle(msgs[i].Buffers[0][:msgs[i].N], rx)
		}
	}
}

// readFromLoop is the portable fallback: one ReadFromUDP syscall per
// datagram, with the literal get-read-parse-put pool cycle.
func (r *Receiver) readFromLoop() {
	defer r.wg.Done()
	for {
		bp := r.pool.Get().(*[]byte)
		n, _, err := r.conn.ReadFromUDP(*bp)
		rx := time.Now()
		if err != nil {
			r.pool.Put(bp)
			if errors.Is(err, net.ErrClosed) {
				return
			}
			r.readErrs.Add(1)
			continue
		}
		r.handle((*bp)[:n], rx)
		// handle copied out everything it needs, so the buffer can go
		// straight back to the pool.
		r.pool.Put(bp)
	}
}

// handle parses one datagram and forwards it. packet.Unmarshal copies
// scalar fields out of b — the Measurement holds no sub-slice of the
// pooled buffer, so recycling the 2KB buffer can never be blocked by a
// 15-byte view of it (the classic "small slice pins big array" leak).
// Malformed input is counted, not fatal: this port faces the network.
func (r *Receiver) handle(b []byte, rx time.Time) {
	r.received.Add(1)
	var pkt packet.Packet
	if err := pkt.Unmarshal(b); err != nil {
		r.parseErrs.Add(1)
		return
	}
	// A full lane drops and counts inside the pipeline; the reader never
	// waits on downstream.
	r.enq.Enqueue(pipeline.Measurement{Pkt: pkt, RxTime: rx})
}
