// Derived from reqfleet/shibuya; modified for engine-sdk.
package pubsub

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
	"github.com/stretchr/testify/require"
)

func serveSubscriber(t *testing.T, listener net.Listener, serve func(net.Conn) error) <-chan error {
	t.Helper()
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			done <- err
			return
		}
		done <- serve(conn)
	}()
	return done
}

func awaitSubscriber(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(6 * time.Second):
		t.Fatal("subscriber peer did not finish")
	}
}

func nextMessage(t *testing.T, messages <-chan protocol.Message) (protocol.Message, bool) {
	t.Helper()
	select {
	case message, ok := <-messages:
		return message, ok
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not deliver or close")
		return nil, false
	}
}

func decodeTopic(conn net.Conn) error {
	var topic protocol.Topic
	if err := json.NewDecoder(conn).Decode(&topic); err != nil {
		return err
	}
	if topic.Name != "engine" || topic.Password != "test-key" {
		return fmt.Errorf("unexpected subscription topic: %q", topic.Name)
	}
	return nil
}

func TestSubscribeConsecutiveJSONAndPeerClose(t *testing.T) {
	for _, ending := range []string{"", "not-json"} {
		t.Run(fmt.Sprintf("ending=%q", ending), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			done := serveSubscriber(t, listener, func(conn net.Conn) error {
				if err := decodeTopic(conn); err != nil {
					return err
				}
				_, err := io.WriteString(conn, `{"verb":"start"}{"verb":"stop"}`+ending)
				return err
			})
			client := PubSubClient{Addr: listener.Addr().String(), Password: "test-key"}
			messages, conn, err := client.Subscribe("engine", func() protocol.Message { return &protocol.Payload{} })
			require.NoError(t, err)
			defer conn.Close()
			for _, verb := range []string{protocol.VerbStart, protocol.VerbStop} {
				message, ok := nextMessage(t, messages)
				require.True(t, ok)
				require.Equal(t, verb, message.(*protocol.Payload).Verb)
			}
			message, ok := nextMessage(t, messages)
			require.False(t, ok)
			require.Nil(t, message)
			awaitSubscriber(t, done)
		})
	}
}

func TestSubscribeLocalCloseClosesChannelAndConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ready := make(chan struct{})
	done := serveSubscriber(t, listener, func(conn net.Conn) error {
		if err := decodeTopic(conn); err != nil {
			return err
		}
		close(ready)
		var b [1]byte
		_, err := conn.Read(b[:])
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("expected client EOF, got %v", err)
		}
		return nil
	})
	client := PubSubClient{Addr: listener.Addr().String(), Password: "test-key"}
	messages, conn, err := client.Subscribe("engine", func() protocol.Message { return &protocol.Payload{} })
	require.NoError(t, err)
	defer conn.Close()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription topic did not arrive")
	}
	require.NoError(t, conn.Close())
	_, ok := nextMessage(t, messages)
	require.False(t, ok)
	awaitSubscriber(t, done)
}

func subscriberCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, roots
}

func TestSubscribeVerifiesTLS(t *testing.T) {
	cert, roots := subscriberCertificate(t)
	for _, tc := range []struct {
		name       string
		roots      *x509.CertPool
		serverName string
		wantError  any
	}{
		{name: "trusted", roots: roots},
		{name: "untrusted", roots: x509.NewCertPool(), wantError: new(x509.UnknownAuthorityError)},
		{name: "wrong identity", roots: roots, serverName: "wrong.example", wantError: new(x509.HostnameError)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
				Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12,
			})
			require.NoError(t, err)
			done := serveSubscriber(t, listener, func(conn net.Conn) error {
				err := conn.(*tls.Conn).Handshake()
				if tc.wantError != nil {
					if err == nil {
						return errors.New("invalid TLS peer completed handshake")
					}
					return nil
				}
				if err != nil {
					return err
				}
				if err := decodeTopic(conn); err != nil {
					return err
				}
				return json.NewEncoder(conn).Encode(protocol.Payload{Verb: protocol.VerbStart})
			})
			client := PubSubClient{
				Addr: listener.Addr().String(), Password: "test-key",
				TLSConfig: &tls.Config{RootCAs: tc.roots, ServerName: tc.serverName, MinVersion: tls.VersionTLS12},
			}
			messages, conn, err := client.Subscribe("engine", func() protocol.Message { return &protocol.Payload{} })
			if tc.wantError != nil {
				require.ErrorAs(t, err, tc.wantError)
				require.Nil(t, messages)
				require.Nil(t, conn)
			} else {
				require.NoError(t, err)
				defer conn.Close()
				message, ok := nextMessage(t, messages)
				require.True(t, ok)
				require.Equal(t, protocol.VerbStart, message.(*protocol.Payload).Verb)
				_, ok = nextMessage(t, messages)
				require.False(t, ok)
			}
			awaitSubscriber(t, done)
		})
	}
}

func TestSubscribeTLSRejectsPlaintextPeer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := serveSubscriber(t, listener, func(conn net.Conn) error {
		var record [1]byte
		if _, err := io.ReadFull(conn, record[:]); err != nil {
			return err
		}
		if record[0] != 22 {
			return fmt.Errorf("expected TLS handshake record, got %d", record[0])
		}
		_, err := io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
		return err
	})
	client := PubSubClient{Addr: listener.Addr().String(), TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	messages, conn, err := client.Subscribe("engine", func() protocol.Message { return &protocol.Payload{} })
	require.Error(t, err)
	require.Nil(t, messages)
	require.Nil(t, conn)
	awaitSubscriber(t, done)
}
