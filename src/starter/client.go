package main

import (
	"fmt"

	clerk "../kvraft"
)

func main() {
	servers := make([]string, 3)
	servers[0] = "127.0.0.1:50051"
	servers[1] = "127.0.0.1:50052"
	servers[2] = "127.0.0.1:50053"
	ck := clerk.MakeClerk(servers)
	for {
		var cmd string
		fmt.Scanf("%s", &cmd)
		// fmt.Printf("%s\n", cmd)
		if cmd == "exit" {
			break
		} else if cmd == "get" {
			var key string
			fmt.Scanf("%s", &key)
			value := ck.Get(key)
			fmt.Printf("%s\n", value)
		} else if cmd == "put" {
			var key string
			var value string
			fmt.Scanf("%s %s", &key, &value)
			// fmt.Printf("%s %s\n", key, value)
			ck.Put(key, value)
		} else if cmd == "append" {
			var key string
			var value string
			fmt.Scanf("%s %s", &key, &value)
			ck.Append(key, value)
		}
	}
}
