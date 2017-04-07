package harness

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"github.com/apache/beam/sdks/go/pkg/beam/typex"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"google.golang.org/grpc"
	"log"
	"reflect"
	"sync"
)

// TODO: figure out semantics and assumptions.

type dataCon struct {
	cc     *grpc.ClientConn
	client pb.BeamFnData_DataClient
}

// DataConnectionManager manages data api connections to the FnHarness.
// Each GRPC port is multiplexed for each bundle.
type DataConnectionManager struct {
	active map[string]dataCon

	mu sync.Mutex
}

func (m *DataConnectionManager) Open(ctx context.Context, id, endpoint string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil {
		m.active = make(map[string]dataCon)
	}
	if _, found := m.active[id]; found {
		return nil // fmt.Errorf("Port %s already present", id)
	}

	cc, err := connect(endpoint, 3)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	client, err := pb.NewBeamFnDataClient(cc).Data(ctx)
	if err != nil {
		cc.Close()
		return fmt.Errorf("Failed to connect to data service: %v", err)
	}

	m.active[id] = dataCon{cc: cc, client: client}
	return nil
}

func (m *DataConnectionManager) Commit(id string, data []*pb.Elements_Data) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	con, ok := m.active[id]
	if !ok {
		return fmt.Errorf("Port %s not found", id)
	}

	msg := &pb.Elements{
		Data: data,
	}

	log.Print("Sending ..")
	return con.client.Send(msg)
}

/*
func (m *DataConnectionManager) Close(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if con, ok := m.active[id]; ok {
		delete(m.active, id)
		return con.cc.Close()
	}
	return nil
}
*/

// TODO: allow abort for both read and write. buffering.

type DataConnectionContext struct {
	InstID string `beam:"data"`
}

func SinkFn(mgr *DataConnectionManager, id string, coder *graph.Coder, t reflect.Type, opt DataConnectionContext, target *pb.Target, in <-chan typex.T) error {
	var stream []byte
	for elm := range in {
		// CAVEAT: the implicit stream coding is simple concatenation of elements (in a nested context).
		// The global window serializes to the empty string. so it is a no-op.

		log.Printf("Sink elm: %v", elm)

		data, err := local.Encode(coder, reflectx.Convert(reflect.ValueOf(elm), t))
		if err != nil {
			return err
		}
		stream = append(stream, data...)
	}

	log.Print("Sink done: %v :: %v", len(stream), stream)

	bundle := []*pb.Elements_Data{
		{
			InstructionReference: opt.InstID,
			Target:               target,
			Data:                 stream,
		},
		{
			InstructionReference: opt.InstID,
			Target:               target,
			// Empty data == sentinel
		},
	}
	return mgr.Commit(id, bundle)
}
