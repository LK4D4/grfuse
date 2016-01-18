package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/LK4D4/grfuse/grpcfs"
	"github.com/LK4D4/grfuse/pb"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: %s <mountpath>")
		os.Exit(1)
	}
	root := os.Args[1]

	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial("127.0.0.1:50000", dialOpts...)
	if err != nil {
		log.Fatal(err)
	}
	cli := pb.NewPathFSClient(conn)
	fs := grpcfs.New(cli)
	nfs := pathfs.NewPathNodeFs(fs, nil)
	server, _, err := nodefs.MountRoot(root, nfs.Root(), nil)
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve()
	log.Printf("Fs mounted to root %s", root)
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt)
	for range sigCh {
		server.Unmount()
		os.Exit(0)
	}
}
