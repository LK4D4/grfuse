package server

import (
	"github.com/LK4D4/grfuse/pb"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"golang.org/x/net/context"
)

type fuseServer struct {
	fs pathfs.FileSystem
}

func fuseContext(gctx *pb.Context) *fuse.Context {
	if gctx == nil {
		return nil
	}
	ctx := &fuse.Context{
		Pid: gctx.Pid,
	}
	if gctx.Owner != nil {
		ctx.Owner = fuse.Owner{
			Uid: gctx.Owner.Uid,
			Gid: gctx.Owner.Gid,
		}
	}
	return ctx
}

func New(fs pathfs.FileSystem) pb.PathFSServer {
	return &fuseServer{fs: fs}
}

func (s *fuseServer) String(ctx context.Context, r *pb.StringRequest) (*pb.StringResponse, error) {
	return nil, nil
}

func (s *fuseServer) SetDebug(ctx context.Context, r *pb.SetDebugRequest) (*pb.SetDebugResponse, error) {
	return nil, nil
}

func (s *fuseServer) GetAttr(ctx context.Context, r *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	attr, code := s.fs.GetAttr(r.Name, fuseContext(r.Context))
	resp := &pb.GetAttrResponse{
		Status: &pb.Status{
			Code: code,
		},
	}
	if code == fuse.OK {
		resp.Attr = &pb.Attr{
			Ino:       attr.Ino,
			SizeAttr:  attr.Size,
			Blocks:    attr.Blocks,
			Atime:     attr.Atime,
			Mtime:     attr.Mtime,
			Ctime:     attr.Ctime,
			Atimensec: attr.Atimensec,
			Mtimensec: attr.Mtimensec,
			Ctimensec: attr.Ctimensec,
			Mode:      attr.Mode,
			Nlink:     attr.Nlink,
			Owner: &pb.Owner{
				Uid: attr.Owner.Uid,
				Gid: attr.Owner.Gid,
			},
			Rdev:    attr.Rdev,
			Blksize: attr.Blksize,
			Padding: attr.Padding,
		}
	}
	return resp, nil
}

func (s *fuseServer) Chmod(ctx context.Context, r *pb.ChmodRequest) (*pb.ChmodResponse, error) {
	return nil, nil
}

func (s *fuseServer) Chown(ctx context.Context, r *pb.ChownRequest) (*pb.ChownResponse, error) {
	return nil, nil
}

func (s *fuseServer) Utimens(ctx context.Context, r *pb.UtimensRequest) (*pb.UtimensResponse, error) {
	return nil, nil
}

func (s *fuseServer) Truncate(ctx context.Context, r *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	return nil, nil
}

func (s *fuseServer) Access(ctx context.Context, r *pb.AccessRequest) (*pb.AccessResponse, error) {
	return nil, nil
}

func (s *fuseServer) Link(ctx context.Context, r *pb.LinkRequest) (*pb.LinkResponse, error) {
	return nil, nil
}

func (s *fuseServer) Mkdir(ctx context.Context, r *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	return nil, nil
}

func (s *fuseServer) Mknod(ctx context.Context, r *pb.MknodRequest) (*pb.MknodResponse, error) {
	return nil, nil
}

func (s *fuseServer) Rename(ctx context.Context, r *pb.RenameRequest) (*pb.RenameResponse, error) {
	return nil, nil
}

func (s *fuseServer) Rmdir(ctx context.Context, r *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	return nil, nil
}

func (s *fuseServer) Unlink(ctx context.Context, r *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	return nil, nil
}

func (s *fuseServer) GetXAttr(ctx context.Context, r *pb.GetXAttrRequest) (*pb.GetXAttrResponse, error) {
	return nil, nil
}

func (s *fuseServer) ListXAttr(ctx context.Context, r *pb.ListXAttrRequest) (*pb.ListXAttrResponse, error) {
	return nil, nil
}

func (s *fuseServer) RemoveXAttr(ctx context.Context, r *pb.RemoveXAttrRequest) (*pb.RemoveXAttrResponse, error) {
	return nil, nil
}

func (s *fuseServer) SetXAttr(ctx context.Context, r *pb.SetXAttrRequest) (*pb.SetXAttrResponse, error) {
	return nil, nil
}

func (s *fuseServer) Open(ctx context.Context, r *pb.OpenRequest) (*pb.OpenResponse, error) {
	f, code := s.fs.Open(r.Name, r.Flags, fuseContext(r.Context))
	resp := &pb.OpenResponse{
		Status: &pb.Status{Code: code},
	}
	if code != fuse.OK {
		return resp, nil
	}
	attr := &fuse.Attr{}
	if code := f.GetAttr(attr); code != fuse.OK {
		return &pb.OpenResponse{
			Status: &pb.Status{Code: code},
		}, nil
	}
	buf := make([]byte, attr.Size)
	readResult, code := f.Read(buf, 0)
	if code != fuse.OK {
		return &pb.OpenResponse{
			Status: &pb.Status{Code: code},
		}, nil
	}
	buf, code = readResult.Bytes(buf)
	if code != fuse.OK {
		return &pb.OpenResponse{
			Status: &pb.Status{Code: code},
		}, nil
	}
	resp.File = &pb.File{
		Data: buf,
	}
	return resp, nil
}

func (s *fuseServer) Create(ctx context.Context, r *pb.CreateRequest) (*pb.CreateResponse, error) {
	return nil, nil
}

func (s *fuseServer) OpenDir(ctx context.Context, r *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	de, code := s.fs.OpenDir(r.Name, fuseContext(r.Context))
	resp := &pb.OpenDirResponse{
		Status: &pb.Status{Code: code},
	}
	if code != fuse.OK {
		return resp, nil
	}
	for _, dir := range de {
		resp.Dirs = append(resp.Dirs, &pb.DirEntry{
			Name: dir.Name,
			Mode: dir.Mode,
		})
	}

	return resp, nil
}

func (s *fuseServer) Symlink(ctx context.Context, r *pb.SymlinkRequest) (*pb.SymlinkResponse, error) {
	return nil, nil
}

func (s *fuseServer) Readlink(ctx context.Context, r *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	return nil, nil
}

func (s *fuseServer) StatFs(ctx context.Context, r *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	return nil, nil
}
