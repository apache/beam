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
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

var (
	provisionCmd = &cobra.Command{
		Use:   "provision",
		Short: "Provision commands",
	}

	infoCmd = &cobra.Command{
		Use:   "info",
		Short: "Retrieve provisioning info",
		RunE:  infoFn,
		Args:  cobra.NoArgs,
	}
)

func init() {
	provisionCmd.AddCommand(infoCmd)
}

func infoFn(cmd *cobra.Command, args []string) error {
	ctx, cc, err := dial()
	if err != nil {
		return err
	}
	defer cc.Close()

	client := pb.NewProvisionServiceClient(cc)

	info, err := client.GetProvisionInfo(ctx, &pb.GetProvisionInfoRequest{})
	if err != nil {
		return err
	}

	cmd.Print(proto.MarshalTextString(info.GetInfo()))
	return nil
}
