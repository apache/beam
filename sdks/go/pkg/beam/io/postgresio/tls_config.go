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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
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

// tlsSettings carries the certificate material needed to build a tls.Config.
type tlsSettings struct {
	Mode     string
	Host     string
	RootCert string
	Cert     string
	Key      string
}

// buildTLSConfig converts sslmode plus certificate paths into a tls.Config.
//
// Returns (nil, nil) when the mode is "disable", signalling that the caller
// must not negotiate TLS at all.
func buildTLSConfig(s tlsSettings) (*tls.Config, error) {
	mode := s.Mode
	if mode == "" {
		mode = DefaultSSLMode
	}
	if err := validateSSLMode(mode); err != nil {
		return nil, err
	}
	if mode == SSLModeDisable {
		return nil, nil
	}

	cfg := &tls.Config{
		ServerName: s.Host,
		MinVersion: tls.VersionTLS12,
	}

	// Load a custom root CA when supplied. Managed providers (Cloud SQL, RDS,
	// Azure Database) present certificates signed by CAs that are not in the
	// system trust store, so without this verify-full cannot succeed against
	// exactly the services most users run.
	var roots *x509.CertPool
	if s.RootCert != "" {
		pem, err := os.ReadFile(s.RootCert)
		if err != nil {
			return nil, fmt.Errorf("failed to read sslrootcert %q: %w", s.RootCert, err)
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("sslrootcert %q contains no valid PEM certificate", s.RootCert)
		}
		cfg.RootCAs = roots
	}

	// Client certificate authentication.
	if (s.Cert == "") != (s.Key == "") {
		return nil, fmt.Errorf("sslcert and sslkey must be supplied together")
	}
	if s.Cert != "" {
		pair, err := tls.LoadX509KeyPair(s.Cert, s.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}

	switch mode {
	case SSLModeRequire:
		// Encrypt without verifying. Explicitly requested by the operator.
		cfg.InsecureSkipVerify = true

	case SSLModeVerifyCA:
		// Verify the chain but not the hostname. Go's standard verifier always
		// checks the hostname when ServerName is set, so the chain check is
		// performed manually with an empty DNSName.
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = verifyChainWithoutHostname(roots)

	case SSLModeVerifyFull:
		// Default Go behaviour: verify chain and hostname against ServerName.
		cfg.InsecureSkipVerify = false
	}

	return cfg, nil
}

// verifyChainWithoutHostname returns a VerifyPeerCertificate function that
// performs full chain validation while skipping hostname matching, which is
// the documented contract of sslmode=verify-ca.
//
// This is needed because setting InsecureSkipVerify disables all of Go's
// verification, so the chain must be re-validated by hand. Omitting this
// would make verify-ca equivalent to require.
func verifyChainWithoutHostname(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("sslmode=verify-ca: server presented no certificate")
		}

		certs := make([]*x509.Certificate, 0, len(rawCerts))
		for i, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("sslmode=verify-ca: failed to parse certificate %d: %w", i, err)
			}
			certs = append(certs, cert)
		}

		intermediates := x509.NewCertPool()
		for _, cert := range certs[1:] {
			intermediates.AddCert(cert)
		}

		// DNSName is intentionally left empty: verify-ca validates the trust
		// chain only.
		if _, err := certs[0].Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
		}); err != nil {
			return fmt.Errorf("sslmode=verify-ca: certificate chain verification failed: %w", err)
		}
		return nil
	}
}
