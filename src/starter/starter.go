package main

import (
	kv "../kvraft"
)

// func startRaft(port string, raft *rf.Raft) (*grpc.Server, net.Listener) {
// 	// 开启raft服务器
// 	listen, err := net.Listen("tcp", port)
// 	if err != nil {
// 		log.Fatalf("Failed to listen: %v", err)
// 	}

// 	// 实例化grpc Server
// 	s := grpc.NewServer()

// 	// 注册HelloService
// 	pb.RegisterRaftServer(s, raft)

// 	log.Println("Listen on " + port)
// 	s.Serve(listen)
// 	return s, listen
// }

func testStartKV() {
	ips := make([]string, 2)
	serverPort := "50050"
	raftPort := "50051"
	ip := "172.18.0.4"
	ips[0] = "172.18.0.2:50051"
	ips[1] = "172.18.0.3:50051"
	kv.StartKVServer(ips, ip, serverPort, raftPort, -1)
}
func main() {

	testStartKV()
	// ip1 := make([]string, 2)
	// port1 := "0.0.0.0:50051"
	// me1 := "172.18.0.4:50051"
	// ip1[0] = "172.18.0.2:50051"
	// ip1[1] = "172.18.0.3:50051"
	// rfRPC1 := rfrpc.MakeRPC("/raft.Raft/")
	// raft1 := rf.Make(ip1, me1, make(chan rf.ApplyMsg, 100), rfRPC1)
	// startRaft(port1, raft1)

	// ip1 := make([]string, 2)
	// ip2 := make([]string, 2)
	// ip3 := make([]string, 2)

	// port1 := "0.0.0.0:50051"
	// me1 := "127.0.0.1:50051"
	// ip1[0] = "127.0.0.1:50052"
	// ip1[1] = "127.0.0.1:50053"
	// rfRPC1 := rfrpc.MakeRPC("/raft.Raft/")

	// port2 := "0.0.0.0:50052"
	// me2 := "127.0.0.1:50052"
	// ip2[0] = "127.0.0.1:50051"
	// ip2[1] = "127.0.0.1:50053"
	// rfRPC2 := rfrpc.MakeRPC("/raft.Raft/")

	// port3 := "0.0.0.0:50053"
	// me3 := "127.0.0.1:50053"
	// ip3[0] = "127.0.0.1:50051"
	// ip3[1] = "127.0.0.1:50052"
	// rfRPC3 := rfrpc.MakeRPC("/raft.Raft/")

	// raft1 := rf.Make(ip1, me1, make(chan rf.ApplyMsg, 100), rfRPC1)
	// raft2 := rf.Make(ip2, me2, make(chan rf.ApplyMsg, 100), rfRPC2)
	// raft3 := rf.Make(ip3, me3, make(chan rf.ApplyMsg, 100), rfRPC3)

	// s1, l1 := startRaft(port1, raft1)
	// s2, l2 := startRaft(port2, raft2)
	// s3, l3 := startRaft(port3, raft3)

	// fmt.Println("启动完成")
	// time.Sleep(1000 * time.Millisecond)
	// var leader int
	// if raft1.IsLeader() {
	// 	s1.Stop()
	// 	l1.Close()
	// 	leader = 1
	// 	rfRPC1.Net = false

	// } else if raft2.IsLeader() {
	// 	s2.Stop()
	// 	l2.Close()
	// 	leader = 2
	// 	rfRPC2.Net = false

	// } else {
	// 	s3.Stop()
	// 	l3.Close()
	// 	leader = 3
	// 	rfRPC3.Net = false
	// }
	// log.Println("关闭节点 " + string(leader))
	// time.Sleep(1000 * time.Millisecond)
	// log.Println("重连节点 " + string(leader))
	// if leader == 1 {
	// 	startRaft(port1, raft1)
	// } else if leader == 2 {
	// 	startRaft(port2, raft2)
	// } else {
	// 	startRaft(port3, raft3)
	// }
	// time.Sleep(1000 * time.Millisecond)
}
