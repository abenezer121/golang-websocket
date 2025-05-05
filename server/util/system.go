package util

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"log"
	"net"
	"os"
	"runtime"
	"syscall"
)

// getFd extracts the underlying file descriptor from a net.Conn.
// The returned file descriptor should be closed when no longer needed,
// except when obtained via syscall.Conn (where the connection maintains ownership).

func GetFd(conn net.Conn) (int, error) {
	// First try the File() method approach for connections that support it
	type filer interface {
		File() (*os.File, error)
	}
	if fc, ok := conn.(filer); ok {
		f, err := fc.File()
		if err != nil {
			return -1, fmt.Errorf("failed to get file from connection: %w", err)
		}

		// Get the original fd
		fd := int(f.Fd())

		// Duplicate the fd because File() transfers ownership and closing the
		// *os.File would close the original connection's fd
		newFd, err := unix.Dup(fd)
		if err != nil {
			f.Close()
			return -1, fmt.Errorf("failed to dup fd %d: %w", fd, err)
		}

		// Close the original file - this won't affect our duplicated fd
		if err := f.Close(); err != nil {
			unix.Close(newFd)
			return -1, fmt.Errorf("failed to close original file: %w", err)
		}

		// fd to non-blocking mode for epoll
		if err := unix.SetNonblock(newFd, true); err != nil {
			unix.Close(newFd)
			return -1, fmt.Errorf("failed to set non-blocking mode: %w", err)
		}

		return newFd, nil
	}

	// Fall back to syscall.Conn for other connection types
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1, errors.New("connection does not implement syscall.Conn or filer interface")
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("failed to get raw connection: %w", err)
	}

	var fd int = -1
	var opErr error
	ctrlErr := rc.Control(func(fdPtr uintptr) {
		//  fd non-blocking - crucial for epoll edge-triggered mode
		err := unix.SetNonblock(int(fdPtr), true)
		if err != nil {
			opErr = fmt.Errorf("failed to set non-blocking: %w", err)
			return
		}
		fd = int(fdPtr)
	})

	if ctrlErr != nil {
		return -1, fmt.Errorf("failed to control raw connection: %w", ctrlErr)
	}
	if opErr != nil {
		return -1, fmt.Errorf("operation error during control: %w", opErr)
	}
	if fd <= 0 {
		return -1, errors.New("invalid file descriptor obtained")
	}

	// Note: For syscall.Conn obtained fds, we don't own the fd so we shouldn't close it
	return fd, nil
}

// setupRlimit increases the open file descriptor limit if possible.
func SetupRlimit(disable bool) {
	// RLIMIT_NOFILE is POSIX, but focus is Linux epoll
	if disable || runtime.GOOS != "linux" {
		log.Println("Skipping RLIMIT_NOFILE adjustment.")
		return
	}

	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		log.Printf("WARN: Failed to get initial RLIMIT_NOFILE: %v", err)
		return
	}
	log.Printf("Initial RLIMIT_NOFILE: Soft=%d, Hard=%d", rlimit.Cur, rlimit.Max)

	desiredLimit := rlimit
	desiredLimit.Cur = desiredLimit.Max

	log.Printf("Attempting to set RLIMIT_NOFILE: Soft=%d, Hard=%d", desiredLimit.Cur, desiredLimit.Max)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
		log.Printf("WARN: Failed to set RLIMIT_NOFILE: %v. Check user permissions.", err)
		if rlimit.Cur < 65536 {
			log.Printf("Attempting to set RLIMIT_NOFILE soft limit to 65536")
			desiredLimit.Cur = 65536
			if desiredLimit.Cur > rlimit.Max {
				desiredLimit.Cur = rlimit.Max
			}
			if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &desiredLimit); err != nil {
				log.Printf("WARN: Failed to set RLIMIT_NOFILE soft limit to %d: %v", desiredLimit.Cur, err)
			}
		}
	}

	var actualLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &actualLimit); err != nil {
		log.Printf("ERROR: Failed to get RLIMIT_NOFILE after setting attempt: %v", err)
	} else {
		log.Printf(">>> Actual RLIMIT_NOFILE after attempt: Soft=%d, Hard=%d <<<", actualLimit.Cur, actualLimit.Max)
		if actualLimit.Cur < 1024 {
			log.Printf("WARNING: Soft limit for open files (%d) is low. Server may hit connection limits.", actualLimit.Cur)
		}
	}
}
