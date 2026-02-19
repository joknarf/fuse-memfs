package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type MemoryFS struct{}

var _ fs.FS = (*MemoryFS)(nil)

// Implement Statfs so df -k works
var _ fs.FSStatfser = (*MemoryFS)(nil)

// Efficient running total of used blocks for Statfs
var usedBlocks uint64 = 0
var usedBlocksMu sync.Mutex

func addUsedBlocks(n uint64) {
	usedBlocksMu.Lock()
	usedBlocks += n
	usedBlocksMu.Unlock()
}

func subUsedBlocks(n uint64) {
	usedBlocksMu.Lock()
	if usedBlocks >= n {
		usedBlocks -= n
	} else {
		usedBlocks = 0
	}
	usedBlocksMu.Unlock()
}

func (fsys MemoryFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	// Try to read MemAvailable from /proc/meminfo
	availMem := uint64(0)
	if f, err := os.Open("/proc/meminfo"); err == nil {
		defer f.Close()
		buf := make([]byte, 256)
		for {
			n, err := f.Read(buf)
			if n == 0 || err != nil {
				break
			}
			lines := string(buf[:n])
			for _, l := range splitLines(lines) {
				if len(l) > 13 && l[:13] == "MemAvailable:" {
					// Format: MemAvailable:  123456 kB
					var kb uint64
					_, err := fmt.Sscanf(l, "MemAvailable: %d kB", &kb)
					if err == nil {
						availMem = kb * 1024
					}
					break
				}
			}
			if availMem > 0 {
				break
			}
		}
	}
	if availMem == 0 {
		// fallback to sysinfo if /proc/meminfo fails
		var sysinfo syscall.Sysinfo_t
		err := syscall.Sysinfo(&sysinfo)
		if err == nil {
			availMem = uint64(sysinfo.Freeram) + uint64(sysinfo.Bufferram)
		} else {
			availMem = 1024 * 1024 * 1024
		}
	}
	usedBlocksMu.Lock()
	ub := usedBlocks
	usedBlocksMu.Unlock()
	blockSize := uint64(1024)
	availBlocks := availMem / blockSize
	resp.Blocks = availBlocks + ub // total = available + used
	resp.Bfree = availBlocks       // free = available
	resp.Bavail = availBlocks
	resp.Files = 1000000
	resp.Ffree = 1000000 - ub
	resp.Bsize = uint32(blockSize) // block size
	resp.Namelen = 255
	resp.Frsize = uint32(blockSize)
	return nil
}

// splitLines splits a string into lines (handles both \n and \r\n)
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func (MemoryFS) Root() (fs.Node, error) {
	root := &Dir{
		files:     make(map[string]fs.Node), // can be *File or *Dir
		mu:        &sync.RWMutex{},
		mtime:     time.Now(),
		ctime:     time.Now(),
		inode:     1,
		parent:    nil,
		sizeDirty: true,
	}
	return root, nil
}

var inodeCounter uint64 = 2 // start at 2, 1 is root
var inodeMu sync.Mutex

func nextInode() uint64 {
	inodeMu.Lock()
	defer inodeMu.Unlock()
	id := inodeCounter
	inodeCounter++
	return id
}

type Dir struct {
	files     map[string]fs.Node // can be *File, *Dir, *Symlink, *Socket
	mu        *sync.RWMutex
	uid       uint32
	gid       uint32
	mode      os.FileMode
	mtime     time.Time
	ctime     time.Time
	inode     uint64
	parent    *Dir   // pointer to parent directory, nil for root
	sizeCache uint64 // cache for directory size
	sizeDirty bool   // mark if size needs recalculation
}

// Socket node
type Socket struct {
	uid   uint32
	gid   uint32
	mode  os.FileMode
	ctime time.Time
	inode uint64
}

var _ fs.Node = (*Socket)(nil)

func (s *Socket) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = s.inode
	a.Mode = os.ModeSocket | 0777
	a.Mtime = s.ctime
	a.Ctime = s.ctime
	a.Uid = s.uid
	a.Gid = s.gid
	a.Size = 0
	a.Blocks = 0
	return nil
}

var _ fs.Node = (*Dir)(nil)
var _ fs.Handle = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)

// Add support for directory creation and removal
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)

// Add support for symlinks
var _ fs.NodeSymlinker = (*Dir)(nil)

// Symlink node
type Symlink struct {
	target string
	uid    uint32
	gid    uint32
	mode   os.FileMode
	ctime  time.Time
	inode  uint64
}

var _ fs.Node = (*Symlink)(nil)
var _ fs.NodeReadlinker = (*Symlink)(nil)

func (s *Symlink) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = s.inode
	a.Mode = os.ModeSymlink | 0777
	a.Mtime = s.ctime
	a.Ctime = s.ctime
	a.Uid = s.uid
	a.Gid = s.gid
	a.Size = uint64(len(s.target))
	return nil
}

func (s *Symlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return s.target, nil
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	a.Inode = d.inode
	if d.mode == 0 {
		a.Mode = os.ModeDir | 0700 // default rwx------
	} else {
		a.Mode = os.ModeDir | d.mode.Perm()
	}
	a.Mtime = d.mtime
	a.Ctime = d.ctime
	if d.uid == 0 && d.gid == 0 {
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	} else {
		a.Uid = d.uid
		a.Gid = d.gid
	}
	// Use cached size if available
	if d.sizeDirty {
		size := 6
		for name := range d.files {
			size += 8 + len(name)
		}
		if size > 192 {
			size = ((size / 4096) + 1) * 4096
		}
		d.sizeCache = uint64(size)
		d.sizeDirty = false
	}
	a.Size = d.sizeCache
	a.Blocks = 1
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if n, ok := d.files[name]; ok {
		return n, nil
	}
	return nil, syscall.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	parentInode := d.inode
	if d.parent != nil {
		parentInode = d.parent.inode
	}
	entries := []fuse.Dirent{
		{Inode: d.inode, Name: ".", Type: fuse.DT_Dir},
		{Inode: parentInode, Name: "..", Type: fuse.DT_Dir},
	}
	for name, node := range d.files {
		var typ fuse.DirentType
		var inode uint64
		switch n := node.(type) {
		case *Dir:
			typ = fuse.DT_Dir
			inode = n.inode
		case *File:
			typ = fuse.DT_File
			inode = n.inode
		case *Symlink:
			typ = fuse.DT_Link
			inode = n.inode
		case *Socket:
			typ = fuse.DT_Socket
			inode = n.inode
		default:
			inode = 0
		}
		entries = append(entries, fuse.Dirent{Inode: inode, Name: name, Type: typ})
	}
	return entries, nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.files[req.Name]; exists {
		return nil, nil, syscall.EEXIST
	}
	now := time.Now()
	// Check for socket creation (mknod with S_IFSOCK)
	if req.Mode&os.ModeSocket != 0 {
		sock := &Socket{
			uid:   req.Header.Uid,
			gid:   req.Header.Gid,
			mode:  req.Mode,
			ctime: now,
			inode: nextInode(),
		}
		d.files[req.Name] = sock
		d.mtime = now
		d.ctime = now
		d.sizeDirty = true
		return sock, nil, nil
	}
	file := &File{content: []byte{}, mu: sync.RWMutex{}, uid: req.Header.Uid, gid: req.Header.Gid, mode: req.Mode, mtime: now, ctime: now, inode: nextInode()}
	d.files[req.Name] = file
	d.mtime = now
	d.ctime = now
	d.sizeDirty = true
	// Account for new file blocks
	addUsedBlocks((0+511)/512 + 7) // empty file, 7 blocks for metadata
	return file, file, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.files[req.Name]; exists {
		return nil, syscall.EEXIST
	}
	now := time.Now()
	dir := &Dir{
		files:     make(map[string]fs.Node),
		mu:        &sync.RWMutex{},
		uid:       req.Header.Uid,
		gid:       req.Header.Gid,
		mode:      req.Mode,
		mtime:     now,
		ctime:     now,
		inode:     nextInode(),
		parent:    d,
		sizeDirty: true,
	}
	d.files[req.Name] = dir
	d.mtime = now
	d.ctime = now
	d.sizeDirty = true
	// Account for new directory blocks
	size := 6
	if size > 192 {
		size = ((size / 4096) + 1) * 4096
	}
	addUsedBlocks((uint64(size) + 511) / 512)
	return dir, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	node, exists := d.files[req.Name]
	if !exists {
		return syscall.ENOENT
	}
	if req.Dir {
		if dir, ok := node.(*Dir); ok {
			if len(dir.files) > 0 {
				return syscall.ENOTEMPTY
			}
			size := 6
			if size > 192 {
				size = ((size / 4096) + 1) * 4096
			}
			subUsedBlocks((uint64(size) + 511) / 512)
		} else {
			return syscall.ENOTDIR
		}
	} else if file, ok := node.(*File); ok {
		file.mu.RLock()
		l := len(file.content)
		file.mu.RUnlock()
		subUsedBlocks((uint64(l)+511)/512 + 7)
	} else if symlink, ok := node.(*Symlink); ok {
		subUsedBlocks((uint64(len(symlink.target)) + 511) / 512)
	} else {
		// No blocks to free for sockets or unknown node types
	}
	delete(d.files, req.Name)
	d.sizeDirty = true
	now := time.Now()
	d.mtime = now
	d.ctime = now
	return nil
}

func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.files[req.NewName]; exists {
		return nil, syscall.EEXIST
	}
	now := time.Now()
	symlink := &Symlink{
		target: req.Target,
		uid:    req.Header.Uid,
		gid:    req.Header.Gid,
		mode:   0777,
		ctime:  now,
		inode:  nextInode(),
	}
	d.files[req.NewName] = symlink
	d.mtime = now
	d.ctime = now
	d.sizeDirty = true
	addUsedBlocks((uint64(len(req.Target)) + 511) / 512)
	return symlink, nil
}

type File struct {
	content    []byte
	mu         sync.RWMutex
	uid        uint32
	gid        uint32
	mode       os.FileMode
	mtime      time.Time
	ctime      time.Time
	inode      uint64
	blockCache uint64 // cache for block usage
	blockDirty bool   // mark if block count needs recalculation
}

var _ fs.Node = (*File)(nil)
var _ fs.Handle = (*File)(nil)
var _ fs.HandleReader = (*File)(nil)
var _ fs.HandleWriter = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

// (No interface assertion for HandleFsyncer; just implement Fsync method)
// Fsync is a no-op for in-memory filesystems, but must be implemented to avoid ENOSYS errors in editors like vi
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// No-op: nothing to flush for in-memory
	return nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a.Inode = f.inode
	if f.mode == 0 {
		a.Mode = 0644
	} else {
		a.Mode = f.mode.Perm()
	}
	a.Size = uint64(len(f.content))
	a.Mtime = f.mtime
	a.Ctime = f.ctime
	a.Uid = f.uid
	a.Gid = f.gid
	if f.blockDirty {
		f.blockCache = (a.Size+511)/512 + 7
		f.blockDirty = false
	}
	a.Blocks = f.blockCache
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if int(req.Offset) >= len(f.content) {
		resp.Data = nil
		return nil
	}
	end := int(req.Offset) + int(req.Size)
	if end > len(f.content) {
		end = len(f.content)
	}
	resp.Data = f.content[req.Offset:end]
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	oldLen := len(f.content)
	end := int(req.Offset) + len(req.Data)
	if end > cap(f.content) {
		// Grow the slice only if needed, double the capacity for amortized O(1) appends
		newCap := end
		if newCap < 2*cap(f.content) {
			newCap = 2 * cap(f.content)
		}
		newContent := make([]byte, end, newCap)
		copy(newContent, f.content)
		f.content = newContent
	} else if end > len(f.content) {
		// Extend the length without allocating
		f.content = f.content[:end]
	}
	// Write the data
	copy(f.content[req.Offset:], req.Data)
	resp.Size = len(req.Data)
	now := time.Now()
	f.mtime = now
	f.ctime = now
	newLen := len(f.content)
	f.blockDirty = true
	f.mu.Unlock()
	// Update used blocks if file grew or shrank
	oldBlocks := (uint64(oldLen)+511)/512 + 7
	newBlocks := (uint64(newLen)+511)/512 + 7
	if newBlocks > oldBlocks {
		addUsedBlocks(newBlocks - oldBlocks)
	} else if newBlocks < oldBlocks {
		subUsedBlocks(oldBlocks - newBlocks)
	}
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	changed := false
	if req.Valid.Mode() {
		f.mode = req.Mode & 0777
		resp.Attr.Mode = f.mode
		changed = true
	}
	if req.Valid.Mtime() {
		f.mtime = req.Mtime
		changed = true
	}
	if changed {
		f.ctime = time.Now()
	}
	f.blockDirty = true
	return nil
}

// Implement Setattr for Dir to support chmod on directories
var _ fs.NodeSetattrer = (*Dir)(nil)

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	changed := false
	if req.Valid.Mode() {
		d.mode = req.Mode & 0777
		resp.Attr.Mode = os.ModeDir | d.mode
		changed = true
	}
	if req.Valid.Mtime() {
		d.mtime = req.Mtime
		changed = true
	}
	if changed {
		d.ctime = time.Now()
	}
	d.sizeDirty = true
	return nil
}

// Add support for renaming files, directories, and symlinks by implementing fs.NodeRenamer for Dir.
var _ fs.NodeRenamer = (*Dir)(nil)

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	d1 := d
	d2, ok := newDir.(*Dir)
	if !ok {
		return syscall.EXDEV // only support Dir to Dir
	}
	// Lock in address order to avoid deadlock
	if d1 == d2 {
		d1.mu.Lock()
		defer d1.mu.Unlock()
	} else if uintptr(unsafe.Pointer(d1)) < uintptr(unsafe.Pointer(d2)) {
		d1.mu.Lock()
		d2.mu.Lock()
		defer d2.mu.Unlock()
		defer d1.mu.Unlock()
	} else {
		d2.mu.Lock()
		d1.mu.Lock()
		defer d1.mu.Unlock()
		defer d2.mu.Unlock()
	}
	entry, exists := d1.files[req.OldName]
	if !exists {
		return syscall.ENOENT
	}
	if _, exists := d2.files[req.NewName]; exists {
		return syscall.EEXIST
	}
	d2.files[req.NewName] = entry
	delete(d1.files, req.OldName)
	now := time.Now()
	d1.mtime = now
	d1.ctime = now
	d2.mtime = now
	d2.ctime = now
	d1.sizeDirty = true
	d2.sizeDirty = true
	return nil
}

// Add support for socket creation (mknod) in directories
var _ fs.NodeMknoder = (*Dir)(nil)

func (d *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.files[req.Name]; exists {
		return nil, syscall.EEXIST
	}
	now := time.Now()
	switch req.Mode & os.ModeType {
	case os.ModeSocket:
		node := &Socket{
			uid:   req.Header.Uid,
			gid:   req.Header.Gid,
			mode:  req.Mode,
			ctime: now,
			inode: nextInode(),
		}
		d.files[req.Name] = node
		d.mtime = now
		d.ctime = now
		d.sizeDirty = true
		return node, nil
	default:
		return nil, syscall.ENOSYS
	}
}

func main() {
	foreground := flag.Bool("foreground", false, "Run in foreground (do not daemonize)")
	notify := flag.Bool("notify", false, "internal: write mount status to fd 3 (used by parent to wait for mount)")
	notifyTimeout := flag.Duration("notify-timeout", 30*time.Second, "how long the parent waits for child mount notification (e.g. 30s, 1m)")
	flag.Parse()

	// Use first positional argument as mountpoint
	if flag.NArg() < 1 {
		log.Fatal("Mount point is required as a positional argument")
	}
	mountPoint := flag.Arg(0)

	if !*foreground {
		// Fork and detach, but notify the parent only after the child has mounted the filesystem.
		if os.Getppid() != 1 {
			exe := os.Args[0]

			// create a pipe so the child can notify the parent about mount status
			pr, pw, err := os.Pipe()
			if err != nil {
				os.Exit(1)
			}

			// Prepare files: child will inherit stdin/stdout/stderr and the write end of the pipe as fd3
			files := []*os.File{os.Stdin, os.Stdout, os.Stderr, pw}

			// Build args: run child in foreground mode and pass flags first so they are parsed,
			// then the mountPoint as the positional argument. Do not append parent's extra args.
			args := []string{exe, "-foreground", "-notify", mountPoint}

			attr := &os.ProcAttr{
				Files: files,
				Env:   os.Environ(),
			}

			proc, err := os.StartProcess(exe, args, attr)
			// close our copy of the write end; child has its own
			pw.Close()
			if err != nil {
				pr.Close()
				os.Exit(1)
			}

			// Wait for a single newline-terminated message from child (with a timeout)
			done := make(chan string, 1)
			go func() {
				// use a buffered reader to read until newline
				r := bufio.NewReader(pr)
				line, err := r.ReadString('\n')
				if err != nil {
					// send whatever we got (possibly empty)
					done <- line
					return
				}
				done <- line
			}()

			select {
			case msg := <-done:
				pr.Close()
				// If child reports OK, parent exits successfully and leaves child running
				if strings.HasPrefix(strings.TrimSpace(msg), "OK") {
					// parent's job is done
					os.Exit(0)
				}
				// Otherwise, print child's message and exit non-zero
				log.Printf("Child failed to mount: %s", strings.TrimSpace(msg))
				// Try to kill child process if still running
				_ = proc.Kill()
				os.Exit(1)
			case <-time.After(*notifyTimeout):
				pr.Close()
				// Timed out waiting for child; try to clean up and exit
				log.Println("Timed out waiting for child to mount filesystem")
				_ = proc.Kill()
				os.Exit(1)
			}
		}
		// Redirect output to /dev/null (only in the daemonized child)
		f, _ := os.OpenFile("/dev/null", os.O_RDWR, 0)
		os.Stdout = f
		os.Stderr = f
		log.SetOutput(f)
	}

	log.Printf("Attempting to mount filesystem at %s\n", mountPoint)
	c, err := fuse.Mount(
		mountPoint,
		fuse.FSName("tmpfs"),
		fuse.Subtype("tmpfs"),
		fuse.WritebackCache(),
		fuse.MaxReadahead(1<<20),
		fuse.AsyncRead(),
	)
	if err != nil {
		// If notify is enabled (child notifying parent on fd 3), write error back before exiting
		if *notify {
			nf := os.NewFile(uintptr(3), "notify")
			if nf != nil {
				fmt.Fprintf(nf, "ERR: %v\n", err)
				nf.Close()
			}
		}
		log.Fatalf("Failed to mount: %v", err)
	}
	defer c.Close()

	// If the parent asked to be notified (we're the child), report success so the parent can exit
	if *notify {
		nf := os.NewFile(uintptr(3), "notify")
		if nf != nil {
			fmt.Fprintln(nf, "OK")
			nf.Close()
		}
	}

	// If running in foreground, handle SIGINT/SIGTERM to unmount
	if *foreground {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-ch
			log.Println("Received interrupt, unmounting...")
			fuse.Unmount(mountPoint)
			os.Exit(0)
		}()
	}

	log.Println("Filesystem mounted successfully, serving...")
	err = fs.Serve(c, MemoryFS{})
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

	// Program will exit when fs.Serve returns (filesystem unmounted)
	log.Println("Filesystem server stopped, exiting.")
}
