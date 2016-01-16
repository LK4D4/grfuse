package grpcfs

import (
	"github.com/LK4D4/grfuse/pb"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"golang.org/x/net/context"
)

type GrpcFs struct {
	client pb.PathFSClient
	pathfs.FileSystem
}

func New(c pb.PathFSClient) *GrpcFs {
	return &GrpcFs{
		client:     c,
		FileSystem: pathfs.NewDefaultFileSystem(),
	}
}

func pbContext(ctx *fuse.Context) *pb.Context {
	if ctx == nil {
		return nil
	}
	return &pb.Context{
		Pid: ctx.Pid,
		Owner: &pb.Owner{
			Uid: ctx.Owner.Uid,
			Gid: ctx.Owner.Gid,
		},
	}
}

func (fs *GrpcFs) GetAttr(name string, ctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	req := &pb.GetAttrRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.GetAttr(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	if resp.Status.Code != fuse.OK {
		return nil, resp.Status.Code
	}
	return &fuse.Attr{
		Ino:       resp.Attr.Ino,
		Size:      resp.Attr.SizeAttr,
		Blocks:    resp.Attr.Blocks,
		Atime:     resp.Attr.Atime,
		Mtime:     resp.Attr.Mtime,
		Ctime:     resp.Attr.Ctime,
		Atimensec: resp.Attr.Atimensec,
		Mtimensec: resp.Attr.Mtimensec,
		Ctimensec: resp.Attr.Ctimensec,
		Mode:      resp.Attr.Mode,
		Nlink:     resp.Attr.Nlink,
		Owner: fuse.Owner{
			Uid: resp.Attr.Owner.Uid,
			Gid: resp.Attr.Owner.Gid,
		},
		Rdev:    resp.Attr.Rdev,
		Blksize: resp.Attr.Blksize,
		Padding: resp.Attr.Padding,
	}, fuse.OK
}

func (fs *GrpcFs) OpenDir(name string, ctx *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	req := &pb.OpenDirRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.OpenDir(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	if resp.Status.Code != fuse.OK {
		return nil, resp.Status.Code
	}
	for _, dir := range resp.Dirs {
		c = append(c, fuse.DirEntry{
			Name: dir.Name,
			Mode: dir.Mode,
		})
	}
	return c, fuse.OK
}

func (fs *GrpcFs) Open(name string, flags uint32, ctx *fuse.Context) (file nodefs.File, code fuse.Status) {
	req := &pb.OpenRequest{
		Name:    name,
		Flags:   flags,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Open(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	if resp.Status.Code != fuse.OK {
		return nil, resp.Status.Code
	}
	return nodefs.NewDataFile(resp.File.Data), fuse.OK
}
