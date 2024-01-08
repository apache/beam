// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/environment"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/prompt"
	udf_v1 "github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/proto/udf/v1"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/udf"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
)

var (
	udfCmd = &cobra.Command{
		Use:   "udf",
		Short: "Manage User Defined Functions",
	}
)

// Service vars
var (
	udfServeCmd = &cobra.Command{
		Use:     "serve",
		Short:   "Run the UDF registry service",
		PreRunE: servePreE,
		RunE:    udfServeE,
	}
)

// Client vars
var (
	tinyGo     environment.Executable = "tinygo"
	src        *udf_v1.UserDefinedFunction
	serviceUrl string
	udfClient  udf_v1.UDFServiceClient

	udfClientCmd = &cobra.Command{
		Use:               "client",
		Short:             "Call a remote UDF registry",
		PersistentPreRunE: clientPreE,
	}

	udfCreateCmd = &cobra.Command{
		Use:   "create FILE",
		Short: "Create a User Defined Function",
		Args:  fileArgs,
		RunE:  udfCreateE,
	}

	udfDeleteCmd = &cobra.Command{
		Use:   "delete URN",
		Short: "Delete a User Defined Function",
		Args:  urnArgs,
		RunE:  udfDeleteE,
	}

	udfUpdateCmd = &cobra.Command{
		Use:   "update FILE",
		Short: "Update a User Defined Function",
		Args:  fileArgs,
		RunE:  udfUpdateE,
	}

	udfDescribeCmd = &cobra.Command{
		Use:   "describe URN",
		Short: "Describe a User Defined Function",
		Args:  urnArgs,
		RunE:  udfDescribeE,
	}
)

func clientPreE(cmd *cobra.Command, _ []string) error {
	conn, err := grpc.DialContext(cmd.Context(), serviceUrl, grpc.WithTransportCredentials(credentials))
	if err != nil {
		return err
	}

	udfClient = udf_v1.NewUDFServiceClient(conn)

	return nil
}

func init() {
	udfCmd.AddCommand(udfServeCmd, udfClientCmd)
	udfClientCmd.AddCommand(udfDeleteCmd, udfCreateCmd, udfDescribeCmd, udfUpdateCmd)
	udfClientCmd.PersistentFlags().StringVar(&serviceUrl, "service", "", "URL of the remote service")
	udfCreateCmd.Flags().StringVar(&urn, "urn", "", "The URN of the User Defined Function.")
	udfUpdateCmd.Flags().StringVar(&urn, "urn", "", "The URN of the User Defined Function.")
}

func fileArgs(_ *cobra.Command, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing FILE")
	}

	path := args[0]
	u, err := readUDF(path)
	src = u

	return err
}

func udfServeE(cmd *cobra.Command, _ []string) error {
	which, err := tinyGo.Which()
	if err != nil {
		return err
	}
	log.Printf("found tinygo at %s", which)
	g := grpc.NewServer()
	ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt)

	defer g.GracefulStop()
	defer cancel()

	if err := udf.Service(g, registry); err != nil {
		return err
	}

	errChan := make(chan error)
	log.Printf("starting udf service at %s\n", address)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	go func() {
		if err := g.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
}

func udfCreateE(cmd *cobra.Command, _ []string) error {
	resp, err := udfClient.Create(cmd.Context(), &udf_v1.CreateRequest{})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

func udfDeleteE(cmd *cobra.Command, _ []string) error {
	yes := false
	if err := prompt.YN(fmt.Sprintf("Are you sure you want to delete %s", urn), &yes); err != nil {
		return err
	}
	if !yes {
		return nil
	}

	resp, err := udfClient.Delete(cmd.Context(), &udf_v1.DeleteRequest{
		Urn: urn,
	})

	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

func udfUpdateE(cmd *cobra.Command, _ []string) error {
	resp, err := udfClient.Update(cmd.Context(), &udf_v1.UpdateRequest{})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

func udfDescribeE(cmd *cobra.Command, _ []string) error {
	resp, err := udfClient.Describe(cmd.Context(), &udf_v1.DescribeRequest{
		Urn: urn,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(resp)
}

func readUDF(path string) (*udf_v1.UserDefinedFunction, error) {
	ext := filepath.Ext(path)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	now := timestamppb.Now()
	return &udf_v1.UserDefinedFunction{
		Bytes:    b,
		Language: language(ext),
		Created:  now,
		Updated:  now,
	}, nil
}

func language(ext string) udf_v1.UserDefinedFunction_Language {
	switch ext {
	case "go":
		return udf_v1.UserDefinedFunction_Language_Go
	case "rs":
		return udf_v1.UserDefinedFunction_Language_Rust
	default:
		return udf_v1.UserDefinedFunction_Language_UNSPECIFIED
	}
}
