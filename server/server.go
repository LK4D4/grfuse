package server

import (
	"time"

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
	return &pb.StringResponse{
		String_: s.fs.String(),
	}, nil
}

func (s *fuseServer) SetDebug(ctx context.Context, r *pb.SetDebugRequest) (*pb.SetDebugResponse, error) {
	s.fs.SetDebug(r.Debug)
	return &pb.SetDebugResponse{}, nil
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
	return &pb.ChmodResponse{
		Status: &pb.Status{Code: s.fs.Chmod(r.Name, r.Mode, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Chown(ctx context.Context, r *pb.ChownRequest) (*pb.ChownResponse, error) {
	return &pb.ChownResponse{
		Status: &pb.Status{Code: s.fs.Chown(r.Name, r.UID, r.GID, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Utimens(ctx context.Context, r *pb.UtimensRequest) (*pb.UtimensResponse, error) {
	atime := time.Unix(0, r.Atime)
	mtime := time.Unix(0, r.Mtime)
	return &pb.UtimensResponse{
		Status: &pb.Status{Code: s.fs.Utimens(r.Name, &atime, &mtime, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Truncate(ctx context.Context, r *pb.TruncateRequest) (*pb.TruncateResponse, error) {
	return &pb.TruncateResponse{
		Status: &pb.Status{Code: s.fs.Truncate(r.Name, r.Size_, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Access(ctx context.Context, r *pb.AccessRequest) (*pb.AccessResponse, error) {
	return &pb.AccessResponse{
		Status: &pb.Status{Code: s.fs.Access(r.Name, r.Mode, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Link(ctx context.Context, r *pb.LinkRequest) (*pb.LinkResponse, error) {
	return &pb.LinkResponse{
		Status: &pb.Status{Code: s.fs.Link(r.OldName, r.NewName, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Mkdir(ctx context.Context, r *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	return &pb.MkdirResponse{
		Status: &pb.Status{Code: s.fs.Mkdir(r.Name, r.Mode, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Mknod(ctx context.Context, r *pb.MknodRequest) (*pb.MknodResponse, error) {
	return &pb.MknodResponse{
		Status: &pb.Status{Code: s.fs.Mknod(r.Name, r.Mode, r.Dev, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Rename(ctx context.Context, r *pb.RenameRequest) (*pb.RenameResponse, error) {
	return &pb.RenameResponse{
		Status: &pb.Status{Code: s.fs.Rename(r.OldName, r.NewName, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Rmdir(ctx context.Context, r *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	return &pb.RmdirResponse{
		Status: &pb.Status{Code: s.fs.Rmdir(r.Name, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Unlink(ctx context.Context, r *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	return &pb.UnlinkResponse{
		Status: &pb.Status{Code: s.fs.Unlink(r.Name, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) GetXAttr(ctx context.Context, r *pb.GetXAttrRequest) (*pb.GetXAttrResponse, error) {
	data, code := s.fs.GetXAttr(r.Name, r.Attribute, fuseContext(r.Context))
	return &pb.GetXAttrResponse{
		Data:   data,
		Status: &pb.Status{Code: code},
	}, nil
}

func (s *fuseServer) ListXAttr(ctx context.Context, r *pb.ListXAttrRequest) (*pb.ListXAttrResponse, error) {
	attrs, code := s.fs.ListXAttr(r.Name, fuseContext(r.Context))
	return &pb.ListXAttrResponse{
		Attributes: attrs,
		Status:     &pb.Status{Code: code},
	}, nil
}

func (s *fuseServer) RemoveXAttr(ctx context.Context, r *pb.RemoveXAttrRequest) (*pb.RemoveXAttrResponse, error) {
	return &pb.RemoveXAttrResponse{
		Status: &pb.Status{Code: s.fs.RemoveXAttr(r.Name, r.Attribute, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) SetXAttr(ctx context.Context, r *pb.SetXAttrRequest) (*pb.SetXAttrResponse, error) {
	return &pb.SetXAttrResponse{
		Status: &pb.Status{Code: s.fs.SetXAttr(r.Name, r.Attribute, r.Data, r.Flags, fuseContext(r.Context))},
	}, nil
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
	// unimplemented until nodefs.File
	return &pb.CreateResponse{
		Status: &pb.Status{Code: fuse.ENOSYS},
	}, nil
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
	return &pb.SymlinkResponse{
		Status: &pb.Status{Code: s.fs.Symlink(r.Value, r.LinkName, fuseContext(r.Context))},
	}, nil
}

func (s *fuseServer) Readlink(ctx context.Context, r *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	val, code := s.fs.Readlink(r.Name, fuseContext(r.Context))
	return &pb.ReadlinkResponse{
		Value:  val,
		Status: &pb.Status{Code: code},
	}, nil
}

func (s *fuseServer) StatFs(ctx context.Context, r *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	statFs := s.fs.StatFs(r.Name)
	if statFs == nil {
		return &pb.StatFsResponse{}, nil
	}
	return &pb.StatFsResponse{
		StatFs: &pb.StatFs{
			Blocks:  statFs.Blocks,
			Bfree:   statFs.Bfree,
			Bavail:  statFs.Bavail,
			Files:   statFs.Files,
			Ffree:   statFs.Ffree,
			Bsize:   statFs.Bsize,
			NameLen: statFs.NameLen,
			Frsize:  statFs.Frsize,
			Padding: statFs.Padding,
			Spare:   statFs.Spare[:],
		},
	}, nil
}
