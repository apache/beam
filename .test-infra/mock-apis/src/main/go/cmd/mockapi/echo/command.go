package echo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	echov1 "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/proto/echo/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	schemeHttp = "http"
	schemeGrpc = "grpc"
)

var (
	callerMap = map[string]caller{
		schemeHttp: httpCaller,
		schemeGrpc: grpcCaller,
	}
	id      string
	u       *url.URL
	r       io.Reader
	Command = &cobra.Command{
		Use: `echo [flags] URL ID [PAYLOAD]

URL:        url of the service (http|grpc)://host/path
ID:         ID of the service quota. See github.com/apache/beam/test-infra/mock-apis/src/main/go/cmd/refresher
[PAYLOAD]:  Payload to send to the echo service; if no PAYLOAD then assumes stdin.`,
		Short: "Call the echo service.",
		RunE:  runE,
		Args:  parseArgs,
	}
)

func parseArgs(_ *cobra.Command, args []string) error {
	var err error
	if len(args) < 2 {
		return fmt.Errorf("fatal: missing required arguments")
	}
	rawUrl := args[0]
	id = args[1]
	u, err = url.Parse(rawUrl)

	if err != nil {
		return err
	}

	r = os.Stdin
	if len(args) == 3 {
		r, err = os.Open(args[2])
	}

	return err
}

func runE(cmd *cobra.Command, _ []string) error {
	f, err := determineCaller()
	if err != nil {
		return err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	req := &echov1.EchoRequest{
		Id:      id,
		Payload: b,
	}
	resp, err := f(cmd.Context(), req)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

type caller func(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error)

func determineCaller() (caller, error) {
	if u == nil {
		return nil, fmt.Errorf("URL is nil")
	}
	f, ok := callerMap[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("could not determine caller from: %s, URL must begin with http or grpc", u.String())
	}
	return f, nil
}

func httpCaller(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	body := bytes.Buffer{}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequest(http.MethodPost, u.String(), &body)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s: %s", resp.Status, string(b))
	}
	//
	//var result *echov1.EchoResponse
	//
	//if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
	//	return nil, err
	//}
	//
	//return result, nil
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(b))
	return &echov1.EchoResponse{}, nil
}

func grpcCaller(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	var opts []grpc.DialOption

	if strings.Contains(u.Host, "localhost") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s%s", u.Host, u.Path), opts...)
	if err != nil {
		return nil, err
	}
	client := echov1.NewEchoServiceClient(conn)
	return client.Echo(ctx, req)
}
