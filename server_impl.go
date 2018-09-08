// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

const bufSize = 1024
const msgSize = 500

// how to know a client is disconnected? if err != nil, it's disconnected

type keyValueServer struct {
	// TODO: implement this!
	listener net.Listener
	// map : key, client conn; value, buffered channel for each connection
	// this buffered channel contains key
	clients          map[net.Conn]client
	quit             bool
	ClientActiveChan chan net.Conn
	ClientDropChan   chan net.Conn
	ClientActive     int
	ClientDrop       int
	operateMap       chan int
}

// every client has a buffered message channel
// check the length of it to implement 8 th
type client struct {
	conn net.Conn
	getRequest chan int
	keys chan string
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	server := keyValueServer{nil, make(map[net.Conn]client), false,
		make(chan net.Conn, 1), make(chan net.Conn, 1),
		0, 0, make(chan int, 1)}
	return &server
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	ln, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	kvs.listener = ln
	init_db()
	go AcceptClients(kvs)
	go HandleConnection(kvs)
	return err
}

func (kvs *keyValueServer) Close() {
	// should close all connections and listener
	kvs.quit = true
	for k, cl := range kvs.clients {
		close(cl.keys)
		k.Close()
	}
	kvs.listener.Close()
	for k, _ := range kvstore {
		clear(k)
		//fmt.Println(k)
		//for i := 0; i < len(values); i++ {
		//	fmt.Println(string(values[i][:]))
		//}
	}
}

func (kvs *keyValueServer) CountActive() int {
	return kvs.ClientActive
}

func (kvs *keyValueServer) CountDropped() int {
	return kvs.ClientDrop
}

// TODO: add additional methods/functions below!
func AcceptClients(kvs *keyValueServer) {
	for {
		conn, err := kvs.listener.Accept()
		if kvs.quit == true {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		kvs.ClientActiveChan <- conn
	}
}

func HandleConnection(kvs *keyValueServer) {
	for {
		select {
		case conn := <-kvs.ClientActiveChan:
			kvs.clients[conn] = client{conn, make(chan int, 1),make(chan string, msgSize)}
			kvs.ClientActive += 1
			go ReadFromClient(kvs, conn)
			go WriteToClient(kvs, conn)
		case conn := <-kvs.ClientDropChan:
			delete(kvs.clients, conn)
			kvs.ClientDrop += 1
			kvs.ClientActive -= 1
		default :
			if kvs.quit == true {
				return
			}
		}
	}
}

// all clients share one put and one delete channel, but can read simultaneously
// op: 0, put; 1, delete; 2, get
func ProcessRequest(kvs *keyValueServer, conn net.Conn, msg [][]byte, op int) {

	key := string(bytes.TrimSuffix(msg[1][:], []byte("\n")))
	switch op {
	case 0:
		<-kvs.operateMap
		put(key, msg[2])
	case 1:
		<-kvs.operateMap
		clear(key)

	case 2:
		<-kvs.clients[conn].getRequest
		if op == 2 && len(kvs.clients[conn].keys) < 500 {

			kvs.clients[conn].keys <- key
		}
	}

	//debugging'

	//values := get(key)
	//for i := 0; i < len(values); i++ {
	//	fmt.Println(string(values[i][:]))
	//}

}

func ReadFromClient(kvs *keyValueServer, conn net.Conn) {
	for {
		//buf := make([]byte, bufSize)
		//_, err := conn.Read(buf)

		str, err := bufio.NewReader(conn).ReadString('\n')
		fmt.Println(str)
		buf := []byte(str)
		if err != nil {
			kvs.ClientDropChan <- conn
			return
		}
		// parse
		msg := bytes.Split(buf, []byte(","))

		// 0, put; 1, delete; 2, get
		op := 0
		method := string(msg[0][:])
		switch method {
		case "put":
			kvs.operateMap <- 1
		case "delete":
			kvs.operateMap <- 1
			op = 1
		case "get":
			kvs.clients[conn].getRequest <- 1
			op = 2
		}
		ProcessRequest(kvs, conn, msg, op)
		//values := get("a")
		//fmt.Println("len of <a>" , ":", len(values))
	}
}

func WriteToClient(kvs *keyValueServer, conn net.Conn) {
	// only stop when disconnected or kvs.clients[conn] closed

	for key := range kvs.clients[conn].keys {
		//fmt.Println(key)
		values := get(key)
		for i := 0; i < len(values); i++ {
			//fmt.Println("write ", i, " times")
			_, err := conn.Write(values[i])
			if err != nil {
				if kvs.quit==false {
					kvs.ClientDropChan <- conn
				}
				return
			}
		}
	}

}
