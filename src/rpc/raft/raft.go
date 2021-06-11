package raft

import (
	"context"

	"google.golang.org/grpc"
)

const (
	raftPrefix = "/raft.Raft/"
	kvPrefix   = "/kv.KV/"
)

func Call(ip string, funcName string, args interface{}, reply interface{}, op string, opts ...grpc.CallOption) error {

	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	// 初始化客户端
	// c := pb.NewRaftClient(conn)

	// err := c.cc.Invoke(context.Background(), prefix+funcName, args, reply, opts...)
	if op == "raft" {
		err = grpc.Invoke(context.Background(), raftPrefix+funcName, args, reply, conn, opts...)
	} else if op == "kv" {
		err = grpc.Invoke(context.Background(), kvPrefix+funcName, args, reply, conn, opts...)
	}

	if err != nil {
		return err
	}
	return nil
}
