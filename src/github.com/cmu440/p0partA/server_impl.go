// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/cmu440/p0partA/kvstore"
	"io"
	"net"
	"strconv"
	"strings"
)

// Make listener var package visible
var listener net.Listener

type keyValueServer struct {
	interactions              chan Interaction
	interactionDone           chan bool
	store                     kvstore.KVStore
	serverBeenClosed          bool
	closeServerChan           chan bool
	serverBeenClosedRequest   chan bool
	serverBeenClosedResponse  chan bool
	serverClosedNotifier      chan bool
	serverStarted             bool
	serverStartChan           chan bool
	serverStartedRequest      chan bool
	serverStartedResponse     chan bool
	activeConnections         int
	activeConnectionsAddChan  chan bool
	activeConnectionsRequest  chan bool
	activeConnectionsResult   chan int
	droppedConnections        int
	droppedConnectionsAddChan chan bool
	droppedConnectionsRequest chan bool
	droppedConnectionsResult  chan int
	connectionsSlice          []net.Conn
	connectionsSliceAdd       chan net.Conn
	connectionsSliceRequest   chan bool
	connectionsSliceResult    chan []net.Conn
}

type GetRequest struct {
	K string
}

type PutRequest struct {
	K string
	V []byte
}

type UpdateRequest struct {
	K  string
	V1 []byte
	V2 []byte
}

type DeleteRequest struct {
	K string
}

type Interaction interface {
	handleInteraction(store kvstore.KVStore)
}

type GetInteraction struct {
	getReq   GetRequest
	response [][]byte
}

func (getInterac *GetInteraction) handleInteraction(store kvstore.KVStore) {
	getInterac.response = store.Get(getInterac.getReq.K)
}

type PutInteraction struct {
	putReq PutRequest
}

func (putInterac *PutInteraction) handleInteraction(store kvstore.KVStore) {
	store.Put(putInterac.putReq.K, putInterac.putReq.V)
}

type UpdateInteraction struct {
	updateReq UpdateRequest
}

func (updateInterac *UpdateInteraction) handleInteraction(store kvstore.KVStore) {
	store.Update(updateInterac.updateReq.K, updateInterac.updateReq.V1, updateInterac.updateReq.V2)
}

type DeleteInteraction struct {
	deleteReq DeleteRequest
}

func (deleteInterac *DeleteInteraction) handleInteraction(store kvstore.KVStore) {
	store.Delete(deleteInterac.deleteReq.K)
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	interactions := make(chan Interaction)

	return &keyValueServer{
		interactions:              interactions,
		interactionDone:           make(chan bool),
		store:                     store,
		closeServerChan:           make(chan bool),
		serverBeenClosedRequest:   make(chan bool),
		serverBeenClosedResponse:  make(chan bool),
		serverClosedNotifier:      make(chan bool),
		serverStartChan:           make(chan bool),
		serverStartedRequest:      make(chan bool),
		serverStartedResponse:     make(chan bool),
		activeConnectionsAddChan:  make(chan bool),
		activeConnectionsRequest:  make(chan bool),
		activeConnectionsResult:   make(chan int),
		droppedConnectionsAddChan: make(chan bool),
		droppedConnectionsRequest: make(chan bool),
		droppedConnectionsResult:  make(chan int),
		connectionsSlice:          make([]net.Conn, 0),
		connectionsSliceAdd:       make(chan net.Conn),
		connectionsSliceRequest:   make(chan bool),
		connectionsSliceResult:    make(chan []net.Conn),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	//Start serverRoutine
	go kvs.serverRoutine()

	//First check if server had been closed before
	kvs.serverBeenClosedRequest <- true
	hasIt := <-kvs.serverBeenClosedResponse
	if hasIt {
		return errors.New("Server has already been closed")
	}
	//Set the server to listen on specified TCP port
	var err error
	listener, err = net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}
	//signal that server has started
	kvs.serverStartChan <- true
	//Pass listener to listener routine which in turn spawns read and write routines
	go kvs.listenerRoutine(listener)
	return nil
}

func (kvs *keyValueServer) listenerRoutine(listener net.Listener) {
	defer listener.Close()
	fmt.Println("Listening on localhost:8080")
	for {
		select {
		case <-kvs.serverClosedNotifier:
			fmt.Println("Server has been closed, exiting listener routine")
			return // Exit the routine if the server has been signaled to close
		default:
			conn, err := listener.Accept()
			if err != nil {
				continue // Skip adding the connection
			}
			kvs.connectionsSliceAdd <- conn

			kvs.activeConnectionsAddChan <- true
			//spin off read and write routine and pass message chan used to communicate between them
			messageChan := make(chan []byte, 500)
			go kvs.readRoutine(conn, messageChan)
			go kvs.writeRoutine(conn, messageChan)
		}
	}
}

func (kvs *keyValueServer) readRoutine(conn net.Conn, messageChan chan<- []byte) {
	for {
		select {
		case <-kvs.serverClosedNotifier:
			fmt.Println("Server has been closed, exiting read routine")
			return // Exit the routine if the server has signaled to close
		default:
			reader := bufio.NewReader(conn)
			for {
				message, err := reader.ReadString('\n') // Read until newline
				if err != nil {
					if err == io.EOF {
						fmt.Println("Connection closed by the peer")
						interac, err1 := requestParser(message)
						if err1 == nil {
							//If Get, handle call and pass message via message chan
							typed, ok := interac.(*GetInteraction)
							if ok {
								kvs.interactions <- typed
								<-kvs.interactionDone
								resp := typed.response
								for _, v := range resp {
									message := fmt.Sprintf("%s:%s\n", typed.getReq.K, string(v))
									byteMessage := []byte(message)
									select {
									case messageChan <- byteMessage:
									default:
										//do nothing, drop message
									}
								}
							} else {
								kvs.interactions <- interac
								<-kvs.interactionDone
							}
						}
						//signal to serverRoutine that this connection has been closed
						kvs.droppedConnectionsAddChan <- true
						fmt.Println("Server has been closed, exiting read routine")
						return
					}
					fmt.Println("Error reading:", err)
					break
				}

				//parse request
				interac, err2 := requestParser(message)
				if err2 == nil {
					//If Get, handle call and pass message via message chan
					typed, ok := interac.(*GetInteraction)
					if ok {
						kvs.interactions <- typed
						<-kvs.interactionDone
						resp := typed.response
						for _, v := range resp {
							message := fmt.Sprintf("%s:%s\n", typed.getReq.K, string(v))
							byteMessage := []byte(message)
							select {
							case messageChan <- byteMessage:
							default:
								//do nothing, drop message
							}
						}
					} else {
						kvs.interactions <- interac
						<-kvs.interactionDone
					}
				} //else do nothing drop message since it can't be parsed(
			}
		}
	}
}

// requestParser parses the provided string into a single Interaction interface
func requestParser(req string) (Interaction, error) {
	// Trim the newline character from the request
	req = strings.TrimSuffix(req, "\n")

	// Split the request into parts by colon
	parts := strings.Split(req, ":")
	if len(parts) < 2 {
		return nil, errors.New("invalid request format")
	}

	command := parts[0]
	key := parts[1]

	switch command {
	case "Put":
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid Put request format for key %s", key)
		}
		value := parts[2]
		return &PutInteraction{putReq: PutRequest{K: key, V: []byte(value)}}, nil
	case "Get":
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Get request format for key %s", key)
		}
		return &GetInteraction{getReq: GetRequest{K: key}}, nil
	case "Delete":
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid Delete request format for key %s", key)
		}
		return &DeleteInteraction{deleteReq: DeleteRequest{K: key}}, nil
	case "Update":
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid Update request format for key %s", key)
		}
		oldValue := parts[2]
		newValue := parts[3]
		return &UpdateInteraction{updateReq: UpdateRequest{K: key, V1: []byte(oldValue), V2: []byte(newValue)}}, nil
	default:
		return nil, fmt.Errorf("unknown command: %s", command)
	}
}

func (kvs *keyValueServer) writeRoutine(conn net.Conn, messageChan <-chan []byte) {
	for {
		select {
		case message := <-messageChan:
			totalSent := 0 // Tracks the total number of bytes sent
			messageLength := len(message)
			for totalSent < messageLength {
				n, err := conn.Write(message[totalSent:])
				if err != nil {
					fmt.Println("Error writing to connection:", err)
					return // Exit the routine on error
				}
				totalSent += n
			}
		case <-kvs.serverClosedNotifier:
			fmt.Println("Server has been closed, exiting write routine")
			return // Exit the routine if the server has been signaled to close
		}
	}
}

func (kvs *keyValueServer) serverRoutine() {
	for {
		select {
		case interaction := <-kvs.interactions:
			interaction.handleInteraction(kvs.store)
			kvs.interactionDone <- true
		case <-kvs.closeServerChan:
			kvs.serverBeenClosed = true
			listener.Close()
			close(kvs.serverClosedNotifier)
			close(kvs.connectionsSliceAdd)
		case <-kvs.serverBeenClosedRequest:
			kvs.serverBeenClosedResponse <- kvs.serverBeenClosed
		case <-kvs.serverStartChan:
			kvs.serverStarted = true
		case <-kvs.serverStartedRequest:
			kvs.serverStartedResponse <- kvs.serverStarted
		case <-kvs.activeConnectionsAddChan:
			kvs.activeConnections += 1
		case <-kvs.activeConnectionsRequest:
			kvs.activeConnectionsResult <- kvs.activeConnections
		case <-kvs.droppedConnectionsAddChan:
			kvs.droppedConnections += 1
			kvs.activeConnections -= 1
		case <-kvs.droppedConnectionsRequest:
			kvs.droppedConnectionsResult <- kvs.droppedConnections
		case conn := <-kvs.connectionsSliceAdd:
			kvs.connectionsSlice = append(kvs.connectionsSlice, conn)
		case <-kvs.connectionsSliceRequest:
			kvs.connectionsSliceResult <- kvs.connectionsSlice
		}
	}
}

func (kvs *keyValueServer) Close() {
	kvs.closeServerChan <- true
	kvs.connectionsSliceRequest <- true
	conns := <-kvs.connectionsSliceResult
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
	return
}

func (kvs *keyValueServer) CountActive() int {
	//check if server has been started
	kvs.serverStartedRequest <- true
	started := <-kvs.serverStartedResponse
	//check if server has been closed
	kvs.serverBeenClosedRequest <- true
	closed := <-kvs.serverBeenClosedResponse
	if !started || closed {
		panic("server is not started or has been closed")
	}
	kvs.activeConnectionsRequest <- true
	return <-kvs.activeConnectionsResult
}

func (kvs *keyValueServer) CountDropped() int {
	//check if server has been started
	kvs.serverStartedRequest <- true
	started := <-kvs.serverStartedResponse
	//check if server has been closed
	kvs.serverBeenClosedRequest <- true
	closed := <-kvs.serverBeenClosedResponse
	if !started || closed {
		panic("server is not started or has been closed")
	}
	kvs.droppedConnectionsRequest <- true
	return <-kvs.droppedConnectionsResult
}
