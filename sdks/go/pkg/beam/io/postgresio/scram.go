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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// SASL mechanism names as advertised by PostgreSQL in AuthenticationSASL.
const (
	mechanismSCRAMSHA256     = "SCRAM-SHA-256"
	mechanismSCRAMSHA256Plus = "SCRAM-SHA-256-PLUS"
)

// scramClient implements the client half of SCRAM-SHA-256 (RFC 5802) as
// profiled by PostgreSQL.
//
// PostgreSQL has defaulted password_encryption to scram-sha-256 since version
// 14, and removed md5 entirely in version 18, so this is the only password
// mechanism that works against a default modern server.
type scramClient struct {
	username string
	password string

	// mechanism is the negotiated mechanism name.
	mechanism string
	// channelBinding carries the gs2 header and, for -PLUS, the binding data.
	gs2Header  string
	cbindInput []byte

	clientNonce string

	// Retained between steps to build the auth message.
	clientFirstBare string
	serverFirst     string
	saltedPassword  []byte
	authMessage     string
}

// newSCRAMClient selects a mechanism from the server's advertised list.
//
// SCRAM-SHA-256-PLUS is preferred when the connection is over TLS and the
// channel binding data is available, because it binds the authentication
// exchange to the specific TLS channel and therefore defeats an
// authentication relay through a man in the middle.
func newSCRAMClient(username, password string, mechanisms []string, cbindData []byte) (*scramClient, error) {
	offersPlain := false
	offersPlus := false
	for _, m := range mechanisms {
		switch m {
		case mechanismSCRAMSHA256:
			offersPlain = true
		case mechanismSCRAMSHA256Plus:
			offersPlus = true
		}
	}
	if !offersPlain && !offersPlus {
		return nil, fmt.Errorf("server advertised no supported SASL mechanism (offered: %v); this connector implements %s and %s",
			mechanisms, mechanismSCRAMSHA256, mechanismSCRAMSHA256Plus)
	}

	nonce, err := generateNonce()
	if err != nil {
		return nil, err
	}

	c := &scramClient{
		username:    username,
		password:    password,
		clientNonce: nonce,
	}

	if offersPlus && len(cbindData) > 0 {
		c.mechanism = mechanismSCRAMSHA256Plus
		c.gs2Header = "p=tls-server-end-point,,"
		c.cbindInput = append([]byte(c.gs2Header), cbindData...)
	} else {
		c.mechanism = mechanismSCRAMSHA256
		// "y" would assert that the client supports channel binding but the
		// server does not. "n" is correct when the client is not using it.
		c.gs2Header = "n,,"
		c.cbindInput = []byte(c.gs2Header)
	}

	return c, nil
}

// Mechanism reports the mechanism selected during construction.
func (c *scramClient) Mechanism() string { return c.mechanism }

// ClientFirst returns the client-first-message.
//
// The username is deliberately sent empty (n=): PostgreSQL takes the user
// from the startup packet and RFC 5802 allows the SCRAM username to be empty
// in that case. This also sidesteps SASLprep normalization of the username.
func (c *scramClient) ClientFirst() string {
	c.clientFirstBare = "n=,r=" + c.clientNonce
	return c.gs2Header + c.clientFirstBare
}

// ClientFinal consumes the server-first-message and produces the
// client-final-message carrying the client proof.
func (c *scramClient) ClientFinal(serverFirst string) (string, error) {
	c.serverFirst = serverFirst

	serverNonce, salt, iterations, err := parseServerFirst(serverFirst)
	if err != nil {
		return "", err
	}

	// The server nonce must start with the client nonce, otherwise the server
	// is replaying a value the client did not generate.
	if !strings.HasPrefix(serverNonce, c.clientNonce) {
		return "", fmt.Errorf("SCRAM: server nonce %q does not extend the client nonce; possible replay", serverNonce)
	}

	c.saltedPassword = pbkdf2SHA256([]byte(saslPrep(c.password)), salt, iterations, sha256.Size)

	clientKey := hmacSHA256(c.saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	cbind := base64.StdEncoding.EncodeToString(c.cbindInput)
	clientFinalWithoutProof := "c=" + cbind + ",r=" + serverNonce

	c.authMessage = c.clientFirstBare + "," + c.serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSHA256(storedKey[:], []byte(c.authMessage))

	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}

	return clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

// VerifyServerFinal checks the server signature in the server-final-message.
//
// Skipping this check would allow a server that does not know the password to
// complete the handshake, defeating the mutual authentication that SCRAM
// exists to provide.
func (c *scramClient) VerifyServerFinal(serverFinal string) error {
	if strings.HasPrefix(serverFinal, "e=") {
		return fmt.Errorf("SCRAM authentication failed: %s", strings.TrimPrefix(serverFinal, "e="))
	}
	if !strings.HasPrefix(serverFinal, "v=") {
		return fmt.Errorf("SCRAM: malformed server-final-message %q", serverFinal)
	}

	got, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(serverFinal, "v="))
	if err != nil {
		return fmt.Errorf("SCRAM: server signature is not valid base64: %w", err)
	}

	serverKey := hmacSHA256(c.saltedPassword, []byte("Server Key"))
	want := hmacSHA256(serverKey, []byte(c.authMessage))

	if subtle.ConstantTimeCompare(got, want) != 1 {
		return fmt.Errorf("SCRAM: server signature mismatch; the server does not know the password")
	}
	return nil
}

// parseServerFirst extracts r=, s= and i= from the server-first-message.
func parseServerFirst(msg string) (nonce string, salt []byte, iterations int, err error) {
	for _, field := range strings.Split(msg, ",") {
		if len(field) < 2 || field[1] != '=' {
			continue
		}
		value := field[2:]
		switch field[0] {
		case 'r':
			nonce = value
		case 's':
			salt, err = base64.StdEncoding.DecodeString(value)
			if err != nil {
				return "", nil, 0, fmt.Errorf("SCRAM: salt is not valid base64: %w", err)
			}
		case 'i':
			iterations, err = strconv.Atoi(value)
			if err != nil {
				return "", nil, 0, fmt.Errorf("SCRAM: iteration count %q is not a number: %w", value, err)
			}
		}
	}

	if nonce == "" {
		return "", nil, 0, fmt.Errorf("SCRAM: server-first-message has no nonce: %q", msg)
	}
	if len(salt) == 0 {
		return "", nil, 0, fmt.Errorf("SCRAM: server-first-message has no salt: %q", msg)
	}
	if iterations <= 0 {
		return "", nil, 0, fmt.Errorf("SCRAM: invalid iteration count %d", iterations)
	}
	return nonce, salt, iterations, nil
}

// generateNonce returns a base64 client nonce drawn from a CSPRNG.
func generateNonce() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("SCRAM: failed to generate nonce: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// pbkdf2SHA256 implements PBKDF2 (RFC 8018) with HMAC-SHA-256.
//
// Implemented locally rather than via golang.org/x/crypto/pbkdf2 so that
// SCRAM support does not introduce a new module dependency into the shared
// Go SDK go.mod.
func pbkdf2SHA256(password, salt []byte, iterations, keyLen int) []byte {
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen

	var out []byte
	buf := make([]byte, 4)

	for block := 1; block <= numBlocks; block++ {
		buf[0] = byte(block >> 24)
		buf[1] = byte(block >> 16)
		buf[2] = byte(block >> 8)
		buf[3] = byte(block)

		u := hmacSHA256(password, append(append([]byte{}, salt...), buf...))
		t := append([]byte{}, u...)

		for i := 1; i < iterations; i++ {
			u = hmacSHA256(password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}

	return out[:keyLen]
}

// saslPrep applies the subset of SASLprep (RFC 4013) that matters here.
//
// A complete implementation requires Unicode normalization tables from
// golang.org/x/text. Passwords that are entirely printable ASCII -- the
// overwhelming majority -- are unaffected by SASLprep, so they are passed
// through unchanged. Non-ASCII passwords are also passed through: PostgreSQL
// itself stores the verifier over the prepared form, so a non-ASCII password
// that requires normalization may fail to authenticate. That is a visible
// failure, not a silent security weakening.
func saslPrep(password string) string {
	for i := 0; i < len(password); i++ {
		if password[i] < 0x20 || password[i] > 0x7e {
			// Non-ASCII or control character: return unchanged.
			return password
		}
	}
	return password
}
