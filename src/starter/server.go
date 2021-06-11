package main

import (
	"log"
	"net"

	kv "../kvraft"
	kvpb "../proto/kv"
	"google.golang.org/grpc"
)

func startKV(port string, kvserver *kv.KVServer) {
	listen, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 实例化grpc Server
	s := grpc.NewServer()

	// 注册HelloService
	kvpb.RegisterKVServer(s, kvserver)

	log.Println("Listen on " + port)
	s.Serve(listen)
}

func main() {
	ips := make([]string, 2)
	serverPort := "50050"
	raftPort := "50050"
	ip := "172.18.0.4"
	ips[0] = "172.18.0.2:50050"
	ips[1] = "172.18.0.3:50050"
	kvserver := kv.StartKVServer(ips, ip, serverPort, raftPort, -1)
	kvserver.StartServer("0.0.0.0:" + serverPort)
}
