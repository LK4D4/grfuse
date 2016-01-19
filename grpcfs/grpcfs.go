package grpcfs

import (
	"log"
	"time"

	"github.com/LK4D4/grfuse/pb"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"golang.org/x/net/context"
)

type GrpcFs struct {
	client pb.PathFSClient
}

func New(c pb.PathFSClient) *GrpcFs {
	return &GrpcFs{
		client: c,
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

func (fs *GrpcFs) OpenDir(name string, ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
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
	var c []fuse.DirEntry
	for _, dir := range resp.Dirs {
		c = append(c, fuse.DirEntry{
			Name: dir.Name,
			Mode: dir.Mode,
		})
	}
	return c, fuse.OK
}

func (fs *GrpcFs) Open(name string, flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
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

func (fs *GrpcFs) String() string {
	resp, err := fs.client.String(context.Background(), nil)
	if err != nil {
		log.Printf("Error calling string method: %v", err)
		return ""
	}
	return resp.String_
}

func (fs *GrpcFs) SetDebug(debug bool) {
	return
}

func (fs *GrpcFs) Chmod(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	req := &pb.ChmodRequest{
		Name:    name,
		Mode:    mode,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Chmod(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Chown(name string, uid uint32, gid uint32, ctx *fuse.Context) fuse.Status {
	req := &pb.ChownRequest{
		Name:    name,
		UID:     uid,
		GID:     gid,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Chown(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Utimens(name string, Atime *time.Time, Mtime *time.Time, ctx *fuse.Context) fuse.Status {
	req := &pb.UtimensRequest{
		Name:    name,
		Atime:   Atime.UnixNano(),
		Mtime:   Mtime.UnixNano(),
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Utimens(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Truncate(name string, size uint64, ctx *fuse.Context) fuse.Status {
	req := &pb.TruncateRequest{
		Name:    name,
		Size_:   size,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Truncate(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Access(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	req := &pb.AccessRequest{
		Name:    name,
		Mode:    mode,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Access(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Link(oldName string, newName string, ctx *fuse.Context) fuse.Status {
	req := &pb.LinkRequest{
		OldName: oldName,
		NewName: newName,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Link(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Mkdir(name string, mode uint32, ctx *fuse.Context) fuse.Status {
	req := &pb.MkdirRequest{
		Name:    name,
		Mode:    mode,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Mkdir(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Mknod(name string, mode uint32, dev uint32, ctx *fuse.Context) fuse.Status {
	req := &pb.MknodRequest{
		Name:    name,
		Mode:    mode,
		Dev:     dev,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Mknod(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Rename(oldName string, newName string, ctx *fuse.Context) fuse.Status {
	req := &pb.RenameRequest{
		OldName: oldName,
		NewName: newName,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Rename(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Rmdir(name string, ctx *fuse.Context) fuse.Status {
	req := &pb.RmdirRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Rmdir(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Unlink(name string, ctx *fuse.Context) fuse.Status {
	req := &pb.UnlinkRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Unlink(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) GetXAttr(name string, attribute string, ctx *fuse.Context) ([]byte, fuse.Status) {
	req := &pb.GetXAttrRequest{
		Name:      name,
		Attribute: attribute,
		Context:   pbContext(ctx),
	}
	resp, err := fs.client.GetXAttr(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return resp.Data, resp.Status.Code
}

func (fs *GrpcFs) ListXAttr(name string, ctx *fuse.Context) ([]string, fuse.Status) {
	req := &pb.ListXAttrRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.ListXAttr(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return resp.Attributes, resp.Status.Code
}

func (fs *GrpcFs) RemoveXAttr(name string, attr string, ctx *fuse.Context) fuse.Status {
	req := &pb.RemoveXAttrRequest{
		Name:      name,
		Attribute: attr,
		Context:   pbContext(ctx),
	}
	resp, err := fs.client.RemoveXAttr(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) SetXAttr(name string, attr string, data []byte, flags int, ctx *fuse.Context) fuse.Status {
	req := &pb.SetXAttrRequest{
		Name:      name,
		Attribute: attr,
		Data:      data,
		Flags:     flags,
		Context:   pbContext(ctx),
	}
	resp, err := fs.client.SetXAttr(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Create(name string, flags uint32, mode uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	req := &pb.CreateRequest{
		Name:    name,
		Flags:   flags,
		Mode:    mode,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Create(context.Background(), req)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return nodefs.NewDataFile(resp.File.Data), resp.Status.Code
}

func (fs *GrpcFs) Symlink(value string, linkName string, ctx *fuse.Context) fuse.Status {
	req := &pb.SymlinkRequest{
		Value:    value,
		LinkName: linkName,
		Context:  pbContext(ctx),
	}
	resp, err := fs.client.Symlink(context.Background(), req)
	if err != nil {
		return fuse.ToStatus(err)
	}
	return resp.Status.Code
}

func (fs *GrpcFs) Readlink(name string, ctx *fuse.Context) (string, fuse.Status) {
	req := &pb.ReadlinkRequest{
		Name:    name,
		Context: pbContext(ctx),
	}
	resp, err := fs.client.Readlink(context.Background(), req)
	if err != nil {
		return "", fuse.ToStatus(err)
	}
	return resp.Value, resp.Status.Code
}

func (fs *GrpcFs) StatFs(name string) *fuse.StatfsOut {
	req := &pb.StatFsRequest{
		Name: name,
	}
	resp, err := fs.client.StatFs(context.Background(), req)
	if err != nil {
		return nil
	}
	spare := [6]uint32{}
	for i, s := range resp.StatFs.Spare {
		if i > 5 {
			break
		}
		spare[i] = s
	}
	return &fuse.StatfsOut{
		Blocks:  resp.StatFs.Blocks,
		Bfree:   resp.StatFs.Bfree,
		Bavail:  resp.StatFs.Bavail,
		Files:   resp.StatFs.Files,
		Ffree:   resp.StatFs.Ffree,
		Bsize:   resp.StatFs.Bsize,
		NameLen: resp.StatFs.NameLen,
		Frsize:  resp.StatFs.Frsize,
		Padding: resp.StatFs.Padding,
		Spare:   spare,
	}
}

func (fs *GrpcFs) OnMount(nodeFs *pathfs.PathNodeFs) {}

func (fs *GrpcFs) OnUnmount() {}
