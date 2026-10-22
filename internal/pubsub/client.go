// Derived from reqfleet/shibuya; modified for engine-sdk.
package pubsub

import (
	"crypto/tls"
	"encoding/json"
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
	t := &protocol.Topic{
		Name:     topic,
		Password: psc.Password,
	}
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(t); err != nil {
		conn.Close()
		return nil, nil, err
	}
	messageChan := make(chan protocol.Message)
	go func(conn net.Conn) {
		defer psc.clean(messageChan, conn)
		decoder := json.NewDecoder(conn)
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
