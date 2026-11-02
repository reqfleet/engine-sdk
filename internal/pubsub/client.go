// Derived from reqfleet/shibuya; modified for engine-sdk.
package pubsub

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/reqfleet/engine-sdk/protocol"
)

type PubSubClient struct {
	Addr     string
	Password string
	// Nil TLSConfig explicitly selects plaintext transport.
	TLSConfig *tls.Config
}

func (psc *PubSubClient) connect() (net.Conn, error) {
	dialer := net.Dialer{Timeout: 2 * time.Second, KeepAlive: time.Second}
	if psc.TLSConfig != nil {
		return tls.DialWithDialer(&dialer, "tcp", psc.Addr, psc.TLSConfig)
	}
	return dialer.Dial("tcp", psc.Addr)
}

func (psc *PubSubClient) clean(messageChan chan protocol.Message, conn net.Conn) {
	close(messageChan)
	conn.Close()
}

func (psc *PubSubClient) Subscribe(topic string, newMessage func() protocol.Message) (chan protocol.Message, net.Conn, error) {
	conn, err := psc.connect()
	if err != nil {
		return nil, nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		conn.Close()
		return nil, nil, err
	}
	t := &protocol.Topic{
		Name:            topic,
		Password:        psc.Password,
		ProtocolVersion: protocol.WireProtocolVersion,
	}
	if err := json.NewEncoder(conn).Encode(t); err != nil {
		conn.Close()
		return nil, nil, err
	}
	// Keep this decoder for commands: the acknowledgement read can buffer the
	// first command when the coordinator writes both JSON values together.
	decoder := json.NewDecoder(conn)
	var response protocol.SubscriptionResponse
	if err := decoder.Decode(&response); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("subscription acknowledgement: %w", err)
	}
	if err := validateSubscriptionResponse(response); err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, nil, err
	}
	messageChan := make(chan protocol.Message)
	go func(conn net.Conn) {
		defer psc.clean(messageChan, conn)
		for {
			value := newMessage()
			if err := decoder.Decode(value); err != nil {
				break
			}
			messageChan <- value
		}
	}(conn)
	return messageChan, conn, nil
}

func validateSubscriptionResponse(response protocol.SubscriptionResponse) error {
	if response.Status == protocol.SubscriptionAccepted &&
		response.ProtocolVersion == protocol.WireProtocolVersion && response.Error == nil {
		return nil
	}
	if response.Status == protocol.SubscriptionRejected && response.Error != nil {
		rejection := response.Error
		if rejection.Code == protocol.UnsupportedProtocolVersion &&
			rejection.ReceivedVersion == protocol.WireProtocolVersion &&
			rejection.Message != "" && len(rejection.SupportedVersions) != 0 {
			for _, version := range rejection.SupportedVersions {
				if version <= 0 || version == protocol.WireProtocolVersion {
					return fmt.Errorf("invalid subscription rejection")
				}
			}
			return rejection
		}
	}
	return fmt.Errorf("invalid subscription acknowledgement")
}
