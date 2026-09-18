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
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// --- Known-answer tests against RFC 7677 ---

// TestSCRAMMatchesRFC7677Vector pins the SCRAM-SHA-256 computation to the
// official test vector from RFC 7677 section 3.
//
// This is a known-answer test against an external reference rather than a
// self-consistency check, so it detects an error in the PBKDF2, HMAC or proof
// derivation that a client/server round trip written against the same code
// would not catch.
//
// The RFC's client-first-bare carries an explicit username ("n=user"), while
// PostgreSQL always sends an empty SCRAM username ("n=") because the user is
// already supplied in the startup packet. clientFirstBare is therefore set
// directly here so that the crypto is compared against the published vector
// without changing the wire behaviour the connector needs.
func TestSCRAMMatchesRFC7677Vector(t *testing.T) {
	const (
		password    = "pencil"
		clientNonce = "rOprNGfwEbeRWgbNEkqO"
		serverFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"

		wantClientFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," +
			"p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
		wantServerSignature = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
	)

	c := &scramClient{
		username:    "user",
		password:    password,
		mechanism:   mechanismSCRAMSHA256,
		gs2Header:   "n,,",
		cbindInput:  []byte("n,,"),
		clientNonce: clientNonce,
	}
	// Match the RFC's client-first-bare, which includes the username.
	c.clientFirstBare = "n=user,r=" + clientNonce

	gotFinal, err := c.ClientFinal(serverFirst)
	if err != nil {
		t.Fatalf("ClientFinal() error: %v", err)
	}
	if gotFinal != wantClientFinal {
		t.Errorf("client-final-message mismatch:\n got %q\nwant %q", gotFinal, wantClientFinal)
	}

	if err := c.VerifyServerFinal(wantServerSignature); err != nil {
		t.Errorf("VerifyServerFinal() rejected the RFC 7677 server signature: %v", err)
	}
}

// TestClientFirstUsesEmptySCRAMUsername documents and pins the PostgreSQL
// convention: the SCRAM username is empty because the startup packet already
// carries it. Sending a non-empty n= here would break authentication against
// a real server.
func TestClientFirstUsesEmptySCRAMUsername(t *testing.T) {
	c := newTestSCRAMClient("pencil")
	got := c.ClientFirst()

	if !strings.HasPrefix(got, "n,,n=,r=") {
		t.Errorf("ClientFirst() = %q, want it to start with \"n,,n=,r=\" (empty SCRAM username)", got)
	}
}

// TestPBKDF2SHA256KnownVectors checks the locally implemented PBKDF2 against
// the RFC 7677 parameters. The expected SaltedPassword was cross-checked with
// an independent implementation (Python hashlib.pbkdf2_hmac).
func TestPBKDF2SHA256KnownVectors(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	if err != nil {
		t.Fatalf("bad test salt: %v", err)
	}

	got := pbkdf2SHA256([]byte("pencil"), salt, 4096, sha256.Size)

	want := []byte{
		0xc4, 0xa4, 0x95, 0x10, 0x32, 0x3a, 0xb4, 0xf9,
		0x52, 0xca, 0xc1, 0xfa, 0x99, 0x44, 0x19, 0x39,
		0xe7, 0x8e, 0xa7, 0x4d, 0x6b, 0xe8, 0x1d, 0xdf,
		0x70, 0x96, 0xe8, 0x75, 0x13, 0xdc, 0x61, 0x5d,
	}

	if len(got) != len(want) {
		t.Fatalf("SaltedPassword length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SaltedPassword mismatch at byte %d:\n got % x\nwant % x", i, got, want)
		}
	}
}

// TestPBKDF2MultiBlockOutput exercises the block-concatenation path, which a
// 32-byte SHA-256 output alone never reaches.
func TestPBKDF2MultiBlockOutput(t *testing.T) {
	got := pbkdf2SHA256([]byte("password"), []byte("salt"), 2, 64)
	if len(got) != 64 {
		t.Fatalf("requested 64 bytes, got %d", len(got))
	}

	// The first 32 bytes must equal a 32-byte derivation with identical inputs.
	first := pbkdf2SHA256([]byte("password"), []byte("salt"), 2, 32)
	for i := range first {
		if got[i] != first[i] {
			t.Fatalf("multi-block output diverges from single-block at byte %d", i)
		}
	}
}

// --- Negative / security tests ---

// TestSCRAMRejectsForgedServerSignature ensures mutual authentication is real.
//
// A server that does not know the password cannot produce a valid server
// signature. If the client accepted any value here, an attacker who
// intercepted the connection could impersonate the database.
func TestSCRAMRejectsForgedServerSignature(t *testing.T) {
	c := newTestSCRAMClient("pencil")
	if _, err := c.ClientFinal("r=" + c.clientNonce + "srv,s=" + base64.StdEncoding.EncodeToString([]byte("saltsalt")) + ",i=4096"); err != nil {
		t.Fatalf("ClientFinal() error: %v", err)
	}

	forged := "v=" + base64.StdEncoding.EncodeToString(make([]byte, sha256.Size))
	if err := c.VerifyServerFinal(forged); err == nil {
		t.Error("VerifyServerFinal() accepted a forged server signature; mutual authentication is not enforced")
	}
}

// TestSCRAMRejectsNonceNotExtendingClientNonce defends against a server that
// replays a nonce the client never generated.
func TestSCRAMRejectsNonceNotExtendingClientNonce(t *testing.T) {
	c := newTestSCRAMClient("pencil")

	bad := "r=totally-different-nonce,s=" + base64.StdEncoding.EncodeToString([]byte("saltsalt")) + ",i=4096"
	if _, err := c.ClientFinal(bad); err == nil {
		t.Error("ClientFinal() accepted a server nonce that does not extend the client nonce")
	}
}

// TestSCRAMSurfacesServerError maps an "e=" server-final into a Go error.
func TestSCRAMSurfacesServerError(t *testing.T) {
	c := newTestSCRAMClient("pencil")
	if _, err := c.ClientFinal("r=" + c.clientNonce + "srv,s=" + base64.StdEncoding.EncodeToString([]byte("saltsalt")) + ",i=4096"); err != nil {
		t.Fatalf("ClientFinal() error: %v", err)
	}

	err := c.VerifyServerFinal("e=invalid-proof")
	if err == nil {
		t.Fatal("VerifyServerFinal() ignored a server error response")
	}
	if !strings.Contains(err.Error(), "invalid-proof") {
		t.Errorf("error does not surface the server reason: %v", err)
	}
}

// TestSCRAMRejectsUnsupportedMechanismList ensures an unusable advertisement
// fails with a clear message instead of a nil dereference.
func TestSCRAMRejectsUnsupportedMechanismList(t *testing.T) {
	if _, err := newSCRAMClient("u", "p", []string{"GSSAPI", "PLAIN"}, nil); err == nil {
		t.Error("newSCRAMClient() accepted a mechanism list with no SCRAM variant")
	}
}

// TestSCRAMPrefersChannelBindingWhenAvailable verifies -PLUS is selected when
// the server offers it and TLS binding data exists, since -PLUS is what
// prevents an authentication relay attack.
func TestSCRAMPrefersChannelBindingWhenAvailable(t *testing.T) {
	withBinding, err := newSCRAMClient("u", "p",
		[]string{mechanismSCRAMSHA256, mechanismSCRAMSHA256Plus}, []byte("cert-hash"))
	if err != nil {
		t.Fatalf("newSCRAMClient() error: %v", err)
	}
	if withBinding.Mechanism() != mechanismSCRAMSHA256Plus {
		t.Errorf("with channel binding available, mechanism = %q, want %q",
			withBinding.Mechanism(), mechanismSCRAMSHA256Plus)
	}
	if !strings.HasPrefix(withBinding.ClientFirst(), "p=tls-server-end-point,,") {
		t.Errorf("-PLUS client-first must carry the p= gs2 header, got %q", withBinding.ClientFirst())
	}

	// Without binding data the client must fall back rather than claim -PLUS.
	plain, err := newSCRAMClient("u", "p",
		[]string{mechanismSCRAMSHA256, mechanismSCRAMSHA256Plus}, nil)
	if err != nil {
		t.Fatalf("newSCRAMClient() error: %v", err)
	}
	if plain.Mechanism() != mechanismSCRAMSHA256 {
		t.Errorf("without binding data, mechanism = %q, want %q", plain.Mechanism(), mechanismSCRAMSHA256)
	}
}

// TestSCRAMNoncesAreUnique guards against a fixed or predictable nonce, which
// would make the client proof replayable.
func TestSCRAMNoncesAreUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		c, err := newSCRAMClient("u", "p", []string{mechanismSCRAMSHA256}, nil)
		if err != nil {
			t.Fatalf("newSCRAMClient() error: %v", err)
		}
		if seen[c.clientNonce] {
			t.Fatalf("duplicate client nonce %q generated within 100 handshakes", c.clientNonce)
		}
		seen[c.clientNonce] = true
	}
}

// --- helpers ---

func newTestSCRAMClient(password string) *scramClient {
	c, err := newSCRAMClient("user", password, []string{mechanismSCRAMSHA256}, nil)
	if err != nil {
		panic(err)
	}
	return c
}

// runSCRAMServer implements the SCRAM-SHA-256 server role over one connection.
//
// It returns nil only when the client's proof verifies against the expected
// password, so a passing client test means the proof was cryptographically
// correct, not merely well-formed.
func runSCRAMServer(conn net.Conn, password string) error {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 1. Read SASLInitialResponse: 'p', len, mechanism\0, int32 len, client-first.
	msgType, payload, err := readPgMessage(conn)
	if err != nil {
		return err
	}
	if msgType != 'p' {
		return errf("expected 'p' SASLInitialResponse, got %q", msgType)
	}

	nul := indexByte(payload, 0)
	if nul < 0 {
		return errf("SASLInitialResponse has no mechanism terminator")
	}
	mechanism := string(payload[:nul])
	if mechanism != mechanismSCRAMSHA256 {
		return errf("client selected unexpected mechanism %q", mechanism)
	}
	rest := payload[nul+1:]
	if len(rest) < 4 {
		return errf("SASLInitialResponse truncated")
	}
	clientFirstLen := int(binary.BigEndian.Uint32(rest[:4]))
	if clientFirstLen != len(rest)-4 {
		return errf("declared client-first length %d does not match %d remaining bytes",
			clientFirstLen, len(rest)-4)
	}
	clientFirst := string(rest[4:])

	// Strip the gs2 header to obtain client-first-bare.
	parts := strings.SplitN(clientFirst, ",", 3)
	if len(parts) != 3 {
		return errf("malformed client-first-message %q", clientFirst)
	}
	clientFirstBare := parts[2]

	var clientNonce string
	for _, f := range strings.Split(clientFirstBare, ",") {
		if strings.HasPrefix(f, "r=") {
			clientNonce = strings.TrimPrefix(f, "r=")
		}
	}
	if clientNonce == "" {
		return errf("client-first-message carried no nonce")
	}

	// 2. Send AuthenticationSASLContinue with server-first-message.
	salt := []byte("\x01\x02\x03\x04\x05\x06\x07\x08")
	const iterations = 4096
	serverNonce := clientNonce + "serverpart"
	serverFirst := "r=" + serverNonce +
		",s=" + base64.StdEncoding.EncodeToString(salt) +
		",i=" + itoa(iterations)

	if err := writeAuthMessage(conn, 11, serverFirst); err != nil {
		return err
	}

	// 3. Read the client-final-message.
	msgType, payload, err = readPgMessage(conn)
	if err != nil {
		return err
	}
	if msgType != 'p' {
		return errf("expected 'p' SASLResponse, got %q", msgType)
	}
	clientFinal := string(payload)

	idx := strings.Index(clientFinal, ",p=")
	if idx < 0 {
		return errf("client-final-message carries no proof: %q", clientFinal)
	}
	clientFinalWithoutProof := clientFinal[:idx]
	gotProof, err := base64.StdEncoding.DecodeString(clientFinal[idx+3:])
	if err != nil {
		return errf("client proof is not valid base64: %v", err)
	}

	// 4. Verify the proof.
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	saltedPassword := pbkdf2SHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientSignature := hmacSHA256(storedKey[:], []byte(authMessage))

	wantProof := make([]byte, len(clientKey))
	for i := range clientKey {
		wantProof[i] = clientKey[i] ^ clientSignature[i]
	}

	if len(gotProof) != len(wantProof) {
		return errf("client proof length %d, want %d", len(gotProof), len(wantProof))
	}
	for i := range wantProof {
		if gotProof[i] != wantProof[i] {
			return errf("client proof does not verify (wrong password or broken derivation)")
		}
	}

	// 5. Send AuthenticationSASLFinal with the server signature.
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	serverSignature := hmacSHA256(serverKey, []byte(authMessage))
	return writeAuthMessage(conn, 12, "v="+base64.StdEncoding.EncodeToString(serverSignature))
}

// writeAuthMessage writes an 'R' authentication message with the given subtype.
func writeAuthMessage(conn net.Conn, authType uint32, body string) error {
	msgLen := 4 + 4 + len(body)
	msg := make([]byte, 0, msgLen+1)
	msg = append(msg, 'R')
	msg = binary.BigEndian.AppendUint32(msg, uint32(msgLen))
	msg = binary.BigEndian.AppendUint32(msg, authType)
	msg = append(msg, body...)
	_, err := conn.Write(msg)
	return err
}

// readPgMessage reads one tagged PostgreSQL protocol message.
func readPgMessage(conn net.Conn) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:5]))
	if length < 4 || length > 1<<20 {
		return 0, nil, errf("implausible message length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil, err
	}
	return header[0], body, nil
}

func indexByte(b []byte, c byte) int {
	return bytes.IndexByte(b, c)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func errf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
