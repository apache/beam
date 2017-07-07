package harness

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/golang/protobuf/ptypes"
	pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_fn_v1"
	"google.golang.org/grpc"
)

// setupRemoteLogging redirects local log messages to FnHarness. It will
// try to reconnect, if a connection goes bad. Falls back to stdout.
func setupRemoteLogging(ctx context.Context, endpoint string) {
	w := &remoteWriter{make(chan *pb.LogEntry, 2000), endpoint}

	// Set up log prefix: "2015/08/24 10:10:55 foo.go:12: Foo .."
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(w)

	go w.Run(ctx)
}

type remoteWriter struct {
	buffer   chan *pb.LogEntry
	endpoint string
}

func (w *remoteWriter) Run(ctx context.Context) error {
	for {
		err := w.connect(ctx)

		fmt.Fprintf(os.Stderr, "Remote logging failed: %v. Retrying in 5 sec ...\n", err)
		time.Sleep(5 * time.Second)
	}
}

func (w *remoteWriter) connect(ctx context.Context) error {
	conn, err := grpc.Dial(w.endpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client, err := pb.NewBeamFnLoggingClient(conn).Logging(ctx)
	if err != nil {
		return err
	}
	defer client.CloseSend()

	for msg := range w.buffer {
		// fmt.Fprintf(os.Stderr, "REMOTE: %v\n", proto.MarshalTextString(msg))

		// TODO: batch up log messages

		list := &pb.LogEntry_List{
			LogEntries: []*pb.LogEntry{msg},
		}

		recordLogEntries(list)

		if err := client.Send(list); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send message: %v\n%v", err, msg)
			return err
		}

		// fmt.Fprintf(os.Stderr, "SENT: %v\n", msg)
	}
	return fmt.Errorf("Internal: buffer closed?")
}

// Match time, such as "2015/08/24 11:30:07.400423 " or "2015/08/24 11:30:07 "
var timeExp = regexp.MustCompile(`\A(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{6})?) `)

// Match source location, such as "main.go:12: "
var lineExp = regexp.MustCompile(`\A(.*\.go:(?:\d*)?): `)

// Write converts a log message and queues it for gRPC delivery. Non-blocking.
//
// The log string will have a (customizable) prefix added by the log.Print
// family as well as a trailing newline, if not already present in the original
// log string. The log string may itself contain newlines and other control
// characters. Fortunately, the log package guarantees that one log invocation
// results in exactly one call to Write, so we do not have to detect log string
// alignment.
//
// For a log.Print("MESSAGE"), we thus expect the message by the
// standard logger to look like:
//
//       "2015/08/24 11:48:18 main.go:16: MESSAGE\n".
//
// We are strict with the expected format and fallback to a simple INFO message
// if we can't parse it. Logs to stderr if buffer is full.
func (w *remoteWriter) Write(p []byte) (n int, err error) {
	raw := p
	if bytes.HasSuffix(raw, []byte{'\n'}) {
		raw = raw[:len(raw)-1]
	}

	now, _ := ptypes.TimestampProto(time.Now())
	entry := &pb.LogEntry{
		Timestamp: now,
		Severity:  pb.LogEntry_INFO,
	}

	if res := timeExp.FindSubmatch(raw); len(res) > 1 {
		if t, err := time.Parse("2006/01/02 15:04:05.000000", string(res[1])); err == nil {
			entry.Timestamp, _ = ptypes.TimestampProto(t)
			raw = raw[len(res[0]):]

			if res := lineExp.FindSubmatch(raw); len(res) > 1 {
				entry.LogLocation = string(res[1])
				raw = raw[len(res[0]):]
			} // else ignore: no source information.
		} // else ignore: cannot parse timestamp
	}

	fmt.Fprintf(os.Stderr, "LOG: %v\n", string(raw))
	entry.Message = string(raw)

	w.buffer <- entry
	return len(p), nil

	/*
		select {
		case :
			return len(p), nil
		case <-time.After(200 * time.Millisecond):
			return fmt.Fprint(os.Stderr, p)
		}
	*/
}
