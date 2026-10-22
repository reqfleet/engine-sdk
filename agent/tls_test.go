// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorRootCAsRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		pem  string
		err  string
	}{
		{name: "missing", err: "missing required coordinator CA certificate: coordinator_ca_cert"},
		{name: "whitespace", pem: " \t\n\r ", err: "missing required coordinator CA certificate: coordinator_ca_cert"},
		{name: "malformed PEM", pem: "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----", err: "invalid coordinator CA certificate: coordinator_ca_cert"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots, err := coordinatorRootCAs(tc.pem)
			require.EqualError(t, err, tc.err)
			require.Nil(t, roots)
		})
	}
}

func newCoordinatorTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func newCoordinatorTestCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serverIP string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP(serverIP)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestAgentFileFetchVerifiesCoordinatorTLS(t *testing.T) {
	ca, caKey, caPEM := newCoordinatorTestCA(t)
	_, _, unrelatedPEM := newCoordinatorTestCA(t)

	for _, tc := range []struct {
		name      string
		caPEM     string
		serverIP  string
		wantError any
	}{
		{name: "trusted coordinator", caPEM: caPEM, serverIP: "127.0.0.1"},
		{name: "untrusted coordinator", caPEM: unrelatedPEM, serverIP: "127.0.0.1", wantError: new(x509.UnknownAuthorityError)},
		{name: "wrong coordinator identity", caPEM: caPEM, serverIP: "192.0.2.1", wantError: new(x509.HostnameError)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert := newCoordinatorTestCertificate(t, ca, caKey, tc.serverIP)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer tls-test" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				if r.URL.Path != protocol.ArtifactPathPrefix+"/test-file" {
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, "coordinator test file")
			}))
			server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
			server.StartTLS()
			defer server.Close()

			t.Setenv("coordinator_ca_cert", tc.caPEM)
			agent := MakeAgentServer(AgentServerOptions{
				EngineMeta: EngineMeta{
					CoordinatorIP: strings.TrimPrefix(server.URL, "https://"),
					APIKey:        "tls-test",
				},
			})
			data, err := agent.cdrclient.FetchFile(context.Background(), agent.reqOpts, "test-file")
			if tc.wantError != nil {
				require.ErrorAs(t, err, tc.wantError)
				require.Nil(t, data)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "coordinator test file", string(data))
		})
	}
}
