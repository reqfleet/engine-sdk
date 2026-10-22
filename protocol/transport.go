// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

/*
reqfleet is having a coordinatoir and engine design.
coordinator and engines are communicated via pubsub through TCP.
More specifically, the coordinator sends commands to the engines via broadcast. The commands are

- start
- stop

In the future, the coordinator could send other types of commands. So we need to have a future-proof design.

So it's better to define specific message types based on the communication needs.

- engines -> EngineMessage. This is the message broadcasted by the coordinator to the engines.
   * engines need to send the EngineMessage for engine topic

*/

type Topic struct {
	Name     string `json:"name"`
	Password string `json:"api_key"`
}

type Message interface {
	// Used by the broker to write messages to the connection
	ToJSON() ([]byte, error)

	// For debugging purposes
	String() string
}
