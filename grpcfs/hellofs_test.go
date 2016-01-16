package grpcfs

import (
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/LK4D4/grfuse/pb"
	"github.com/LK4D4/grfuse/server"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"google.golang.org/grpc"
)

type HelloFs struct {
	pathfs.FileSystem
}

func (me *HelloFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	switch name {
	case "file.txt":
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0644, Size: uint64(len(name)),
		}, fuse.OK
	case "":
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (me *HelloFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		c = []fuse.DirEntry{{Name: "file.txt", Mode: fuse.S_IFREG}}
		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (me *HelloFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	if name != "file.txt" {
		return nil, fuse.ENOENT
	}
	if flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	return nodefs.NewDataFile([]byte(name)), fuse.OK
}

type helloServer struct {
	fsSrv *fuse.Server
	gSrv  *grpc.Server
	addr  net.Addr
}

func (h *helloServer) Close() {
	h.fsSrv.Unmount()
	h.gSrv.Stop()
}

type helloClient struct {
	fsSrv *fuse.Server
}

func (h *helloClient) Close() {
	h.fsSrv.Unmount()
}

func startServer(root string, fs pathfs.FileSystem) (*helloServer, error) {
	nfs := pathfs.NewPathNodeFs(fs, nil)
	fuseSrv, _, err := nodefs.MountRoot(root, nfs.Root(), nil)
	if err != nil {
		return nil, err
	}
	go fuseSrv.Serve()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	pb.RegisterPathFSServer(s, server.New(fs))
	go s.Serve(l)
	return &helloServer{
		fsSrv: fuseSrv,
		gSrv:  s,
		addr:  l.Addr(),
	}, nil
}

func startFs(root, address string) (*helloClient, error) {
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(address, dialOpts...)
	if err != nil {
		return nil, err
	}
	cli := pb.NewPathFSClient(conn)
	fs := New(cli)
	nfs := pathfs.NewPathNodeFs(fs, nil)
	server, _, err := nodefs.MountRoot(root, nfs.Root(), nil)
	if err != nil {
		return nil, err
	}
	go server.Serve()
	return &helloClient{
		fsSrv: server,
	}, nil
}

func TestHelloWorld(t *testing.T) {
	tmpSrv, err := ioutil.TempDir("", "fuse-server-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpSrv)
	hfs := &HelloFs{FileSystem: pathfs.NewDefaultFileSystem()}
	hsrv, err := startServer(tmpSrv, hfs)
	if err != nil {
		t.Fatal(err)
	}
	defer hsrv.Close()
	tmpCli, err := ioutil.TempDir("", "fuse-client-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpCli)
	hcli, err := startFs(tmpCli, hsrv.addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer hcli.Close()
	fis, err := ioutil.ReadDir(tmpCli)
	if err != nil {
		t.Fatal(err)
	}
	if len(fis) != 1 {
		t.Fatalf("expected to find one file, got %d", len(fis))
	}
	fi := fis[0]
	if fi.IsDir() {
		t.Fatal("should be a file, got dir")
	}
	if fi.Name() != "file.txt" {
		t.Fatalf("file should be \"file.txt\", got %s", fi.Name())
	}
	f, err := os.Open(filepath.Join(tmpCli, fi.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "file.txt" {
		t.Fatalf("file content expected to be \"file.txt\", got %s", data)
	}
	if f, err := os.Open("whatever"); !os.IsNotExist(err) {
		if err == nil {
			f.Close()
		}
		t.Fatalf("error from open should be not exist, got %v", err)
	}
}
