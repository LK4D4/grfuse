package grpcfs

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/LK4D4/grfuse/pb"
	"github.com/LK4D4/grfuse/server"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"google.golang.org/grpc"
)

type loopbackServer struct {
	*grpc.Server
	Addr string
}

func startLoopbackServer(root string) (*loopbackServer, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	nfs := pathfs.NewLoopbackFileSystem(root)
	pb.RegisterPathFSServer(s, server.New(nfs))
	go s.Serve(l)
	return &loopbackServer{
		Server: s,
		Addr:   l.Addr().String(),
	}, nil
}

type roots struct {
	srv string
	cli string
}

func testMkdir(t *testing.T, r roots) {
	if err := os.Mkdir(filepath.Join(r.cli, "mkdir"), 0777); err != nil {
		t.Fatal(err)
	}
	di, err := os.Stat(filepath.Join(r.srv, "mkdir"))
	if err != nil {
		t.Fatal(err)
	}
	if !di.IsDir() {
		t.Fatal("should be a directory")
	}
}

func TestPathOps(t *testing.T) {
	tmpSrv, err := ioutil.TempDir("", "fuse-server-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpSrv)
	tmpCli, err := ioutil.TempDir("", "fuse-client-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpCli)
	s, err := startLoopbackServer(tmpSrv)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	cliFs, err := startFs(tmpCli, s.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cliFs.Close()
	r := roots{
		srv: tmpSrv,
		cli: tmpCli,
	}
	testMkdir(t, r)
}
