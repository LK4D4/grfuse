package grpcfs

import (
	"github.com/LK4D4/grfuse/pb"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"google.golang.org/grpc"
)

type fuseClient struct {
	srv  *fuse.Server
	conn *grpc.ClientConn
}

func (c *fuseClient) Close() {
	c.srv.Unmount()
	c.conn.Close()
}

func startFs(root, address string) (*fuseClient, error) {
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
	return &fuseClient{
		srv:  server,
		conn: conn,
	}, nil
}
