//go:build integration
// +build integration

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

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	TIMEOUT_HTTP    = 10 * time.Second
	TIMEOUT_STARTUP = 30 * time.Second
)

type EmulatorClient struct {
	host   string
	client *http.Client
}

func makeEmulatorCiient() *EmulatorClient {
	return &EmulatorClient{
		os.Getenv("FIREBASE_AUTH_EMULATOR_HOST"),
		&http.Client{Timeout: TIMEOUT_HTTP},
	}
}

func (e *EmulatorClient) waitApi() {
	terminate := time.NewTimer(TIMEOUT_STARTUP)
	tick := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-terminate.C:
			log.Fatalf("timeout waiting for emulator")
		case <-tick.C:
			resp, err := e.do(http.MethodGet, "", nil)
			if err != nil {
				log.Println("emulator API:", err)
				continue
			}
			parsed := struct {
				AuthEmulator struct {
					Ready bool `json:"ready"`
				} `json:"authEmulator"`
			}{}
			err = json.Unmarshal(resp, &parsed)
			if err != nil {
				log.Println("emulator API bad response:", err)
				continue
			}
			if parsed.AuthEmulator.Ready {
				return
			}
		}
	}
}

func (e *EmulatorClient) do(method, endpoint string, jsonBody map[string]string) ([]byte, error) {
	url := "http://" + e.host
	if endpoint > "" {
		url += "/" + endpoint
	}
	var buf []byte
	// handle nil jsonBody as no body
	if jsonBody != nil {
		buf, _ = json.Marshal(jsonBody)
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Add("content-type", "application/json")

	response, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}

	// Close the connection to reuse it
	defer response.Body.Close()
	// show the response in stdout
	tee := io.TeeReader(response.Body, os.Stdout)
	defer os.Stdout.WriteString("\n")

	var out []byte
	out, err = io.ReadAll(tee)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Get valid Firebase ID token
// Simulate Frontend client authorization logic
// Here, we use the simplest possible authorization: email/password
// Firebase Admin SDK lacks methods to create a user and get ID token
func (e *EmulatorClient) getIDToken(email string) string {
	// create a user (sign-up with dummy email/password)
	endpoint := "identitytoolkit.googleapis.com/v1/accounts:signUp?key=anything_goes"
	body := map[string]string{"email": email, "password": "1q2w3e"}
	resp, err := e.do(http.MethodPost, endpoint, body)
	if err != nil {
		log.Fatalf("emulator request error: %+v", err)
	}

	var parsed struct {
		IdToken string `json:"idToken"`
	}
	err = json.Unmarshal(resp, &parsed)
	if err != nil {
		log.Fatalf("failed to parse output: %+v", err)
	}

	return parsed.IdToken
}
