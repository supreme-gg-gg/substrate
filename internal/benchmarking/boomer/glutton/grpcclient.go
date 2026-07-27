// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package glutton

import (
	"fmt"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const (
	ateapiServerCA         = "/run/servicedns-ca/ca.crt"
	ateapiClientCredBundle = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
)

// DialControl opens an authenticated mTLS gRPC connection to ateapi.
func DialControl(endpoint string) (*grpc.ClientConn, ateapipb.ControlClient, error) {
	opts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           ateapiServerCA,
		ServerName:       "api.ate-system.svc",
		ClientCredBundle: ateapiClientCredBundle,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure ateapi credentials: %w", err)
	}
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}
	return conn, ateapipb.NewControlClient(conn), nil
}
