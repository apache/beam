package harness

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/exec"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
)

// DataManager manages data channels to the FnHarness. A fixed number of channels
// are generally used, each managing multiple logical byte streams.
type DataManager struct {
	ports map[string]*DataChannel
	mu    sync.Mutex
}

func (m *DataManager) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	ch, err := m.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenRead(ctx, id)
}

func (m *DataManager) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	ch, err := m.open(ctx, id.Port)
	if err != nil {
		return nil, err
	}
	return ch.OpenWrite(ctx, id)
}

func (m *DataManager) open(ctx context.Context, port graph.Port) (*DataChannel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ports == nil {
		m.ports = make(map[string]*DataChannel)
	}
	if con, ok := m.ports[port.ID]; ok {
		return con, nil
	}

	ch, err := NewDataChannel(ctx, port)
	if err != nil {
		return nil, err
	}
	m.ports[port.ID] = ch
	return ch, nil
}

// DataChannel manages a single grpc connection to the FnHarness.
type DataChannel struct {
	cc     *grpc.ClientConn
	client pb.BeamFnData_DataClient
	port   graph.Port

	writers map[string]*dataWriter
	readers map[string]*dataReader
	// TODO: early/late closed, bad instructions, finer locks, reconnect?

	mu sync.Mutex
}

func NewDataChannel(ctx context.Context, port graph.Port) (*DataChannel, error) {
	cc, err := connect(port.URL, 3)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	client, err := pb.NewBeamFnDataClient(cc).Data(ctx)
	if err != nil {
		cc.Close()
		return nil, fmt.Errorf("failed to connect to data service: %v", err)
	}

	ret := &DataChannel{
		cc:      cc,
		client:  client,
		port:    port,
		writers: make(map[string]*dataWriter),
		readers: make(map[string]*dataReader),
	}
	go ret.read(ctx)

	return ret, nil
}

func (m *DataChannel) OpenRead(ctx context.Context, id exec.StreamID) (io.ReadCloser, error) {
	return m.makeReader(ctx, id), nil
}

func (m *DataChannel) OpenWrite(ctx context.Context, id exec.StreamID) (io.WriteCloser, error) {
	return m.makeWriter(ctx, id), nil
}

func (m *DataChannel) read(ctx context.Context) {
	cache := make(map[string]*dataReader)
	for {
		msg, err := m.client.Recv()
		if err != nil {
			if err == io.EOF {
				// TODO: can this happen before shutdown? Reconnect?
				log.Printf("DataChannel %v closed", m.port)
				return
			}
			panic(fmt.Errorf("channel %v bad: %v", m.port, err))
		}

		// Each message may contain segments for multiple streams, so we
		// must treat each segment in isolation. We maintain a local cache
		// to reduce lock contention.

		for _, elm := range msg.GetData() {
			id := exec.StreamID{m.port, graph.Target{elm.GetTarget().PrimitiveTransformReference, elm.GetTarget().GetName()}, elm.GetInstructionReference()}
			sid := id.String()

			var r *dataReader
			if local, ok := cache[sid]; ok {
				r = local
			} else {
				r = m.makeReader(ctx, id)
				cache[sid] = r
			}

			if len(elm.GetData()) == 0 {
				// Sentinel EOF segment for stream. Close buffer to signal EOF.
				close(r.buf)

				// Clean up channel bookkeeping. We'll never see another message
				// for it again.
				delete(cache, sid)
				m.mu.Lock()
				delete(m.readers, sid)
				m.mu.Unlock()
				continue
			}

			// This send is deliberately blocking, if we exceed the buffering for
			// a reader. We can't buffer the entire main input, if some user code
			// is slow (or gets stuck).

			r.buf <- elm.GetData()
		}
	}
}

func (m *DataChannel) makeReader(ctx context.Context, id exec.StreamID) *dataReader {
	m.mu.Lock()
	defer m.mu.Unlock()

	sid := id.String()
	if r, ok := m.readers[sid]; ok {
		return r
	}

	r := &dataReader{buf: make(chan []byte, 20)}
	m.readers[sid] = r
	return r
}

func (m *DataChannel) makeWriter(ctx context.Context, id exec.StreamID) *dataWriter {
	m.mu.Lock()
	defer m.mu.Unlock()

	sid := id.String()
	if w, ok := m.writers[sid]; ok {
		return w
	}

	w := &dataWriter{ch: m, id: id}
	m.writers[sid] = w
	return w
}

type dataReader struct {
	buf chan []byte
	cur []byte
}

func (r *dataReader) Close() error {
	// TODO: allow early close to throw away data async
	return nil
}

func (r *dataReader) Read(buf []byte) (int, error) {
	if r.cur == nil {
		b, ok := <-r.buf
		if !ok {
			return 0, io.EOF
		}
		r.cur = b
	}

	n := len(buf)
	if len(r.cur) < n {
		n = len(r.cur)
	}

	for i := 0; i < n; i++ {
		buf[i] = r.cur[i]
	}

	if len(r.cur) == n {
		r.cur = nil
	} else {
		r.cur = r.cur[n:]
	}
	return n, nil
}

// TODO: less naive writer.

type dataWriter struct {
	buf []byte

	id exec.StreamID
	ch *DataChannel
}

func (w *dataWriter) Close() error {
	w.ch.mu.Lock()
	defer w.ch.mu.Unlock()

	target := &pb.Target{PrimitiveTransformReference: w.id.Target.ID, Name: w.id.Target.Name}
	msg := &pb.Elements{
		Data: []*pb.Elements_Data{
			{
				InstructionReference: w.id.InstID,
				Target:               target,
				Data:                 w.buf,
			},
			{
				InstructionReference: w.id.InstID,
				Target:               target,
				// Empty data == sentinel
			},
		},
	}
	w.buf = nil
	delete(w.ch.writers, w.id.String())

	log.Print("Sending ..")
	return w.ch.client.Send(msg)
}

func (w *dataWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

type DataConnectionContext struct {
	InstID string `beam:"opt"`
}
