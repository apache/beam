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

package postgresio

import (
	"fmt"
)

// SSL modes supported by this connector. These mirror the libpq sslmode
// values that carry a well-defined verification contract. The libpq modes
// "allow" and "prefer" are deliberately not supported: both permit a silent
// downgrade to cleartext, which makes the security posture of a pipeline
// depend on server configuration rather than on pipeline configuration.
const (
	// SSLModeDisable performs no TLS negotiation at all.
	SSLModeDisable = "disable"
	// SSLModeRequire encrypts the connection but verifies nothing. It stops
	// passive eavesdropping only; it does not stop an active man in the middle.
	SSLModeRequire = "require"
	// SSLModeVerifyCA verifies that the server certificate chains to a trusted
	// CA but does not check the hostname.
	SSLModeVerifyCA = "verify-ca"
	// SSLModeVerifyFull verifies the chain and the hostname. This is the default.
	SSLModeVerifyFull = "verify-full"
)

// DefaultSSLMode is the mode applied when a caller does not specify one.
//
// verify-full is chosen over verify-ca because managed PostgreSQL providers
// sign every tenant's server certificate with a shared regional CA. Under
// verify-ca a certificate issued to any other customer of the same provider
// validates successfully, so verify-ca provides no tenant isolation. The two
// modes differ by a single hostname comparison performed once per handshake,
// so there is no meaningful performance argument for the weaker mode.
const DefaultSSLMode = SSLModeVerifyFull

// validSSLModes is the closed set accepted by validateSSLMode.
var validSSLModes = map[string]bool{
	SSLModeDisable:    true,
	SSLModeRequire:    true,
	SSLModeVerifyCA:   true,
	SSLModeVerifyFull: true,
}

// validateSSLMode rejects any mode outside the supported set.
//
// An unrecognized value must be a construction-time error rather than a
// silent fallback: a typo such as "verify_full" or "VerifyFull" would
// otherwise be treated as "not verify-full" and downgrade the connection.
func validateSSLMode(mode string) error {
	if mode == "" {
		return nil // an empty mode is normalized to DefaultSSLMode by the option constructors.
	}
	if !validSSLModes[mode] {
		return fmt.Errorf("invalid sslmode %q: supported modes are %q, %q, %q and %q",
			mode, SSLModeDisable, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull)
	}
	return nil
}
