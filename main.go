package main

import (
    "fmt"
    "golang.org/x/sys/unix"
    "net"
    "os"
    "os/signal"
    "syscall"
)

const maxEvents = 10
const bufferSize = 1024

var listenerFd int

func main() {
    epollFd, err := unix.EpollCreate1(0)
    if err != nil {
        fmt.Printf("Failed to create epoll instance: %v\n", err)
        return
    }
    defer unix.Close(epollFd)
    fmt.Printf("Created epoll instance: %d\n", epollFd)

    // Create a TCP listener
    listener, err := net.Listen("tcp", ":6379") // Use Redis default port for example
    if err != nil {
        fmt.Printf("Failed to create listener: %v\n", err)
        return
    }
    defer listener.Close()
    fmt.Println("TCP listener created on port 6379")

    listenerFile, err := listener.(*net.TCPListener).File()
    if err != nil {
        fmt.Printf("Failed to get file descriptor: %v\n", err)
        return
    }
    listenerFd = int(listenerFile.Fd())
    fmt.Printf("Listener file descriptor: %d\n", listenerFd)

    // Set listener to non-blocking mode
    unix.SetNonblock(listenerFd, true)
    fmt.Println("Set listener to non-blocking mode")

    // Register listener file descriptor with epoll
    event := &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(listenerFd)}
    if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, listenerFd, event); err != nil {
        fmt.Printf("Failed to add listener file descriptor to epoll: %v\n", err)
        return
    }
    fmt.Println("Registered listener file descriptor with epoll")

    // Setting up signal handling to terminate the program gracefully
    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    done := make(chan bool, 1)

    go func() {
        <-sigs
        done <- true
    }()

    // Event loop
    events := make([]unix.EpollEvent, maxEvents)
    fmt.Println("Entering event loop")
    for {
        select {
        case <-done:
            fmt.Println("Terminating...")
            return
        default:
            n, err := unix.EpollWait(epollFd, events, -1)
            if err != nil {
                if err == unix.EINTR {
                    continue
                }
                fmt.Printf("EpollWait error: %v\n", err)
                return
            }

            for i := 0; i < n; i++ {
                handleEvent(epollFd, events[i].Fd, events[i].Events, listener)
            }
        }
    }
}

func handleEvent(epollFd int, fd int32, events uint32, listener net.Listener) {
    if events&unix.EPOLLIN != 0 {
        if fd == int32(listenerFd) {
            handleAccept(epollFd, listener)
        } else {
            handleRead(epollFd, int(fd))
        }
    }
}

func handleAccept(epollFd int, listener net.Listener) {
    conn, err := listener.Accept()
    if err != nil {
        fmt.Printf("Accept error: %v\n", err)
        return
    }
    connFdFile, err := conn.(*net.TCPConn).File()
    if err != nil {
        fmt.Printf("Failed to get connection file descriptor: %v\n", err)
        return
    }
    connFd := int(connFdFile.Fd())
    unix.SetNonblock(connFd, true)
    event := &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(connFd)}
    if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, connFd, event); err != nil {
        fmt.Printf("Failed to add connection file descriptor to epoll: %v\n", err)
        return
    }
    fmt.Printf("Accepted connection from %s, connFd: %d\n", conn.RemoteAddr(), connFd)
}

func handleRead(epollFd int, fd int) {
    buffer := make([]byte, bufferSize)
    count, err := unix.Read(fd, buffer)
    if err != nil {
        fmt.Printf("Read error: %v\n", err)
        closeClientConnection(epollFd, fd)
        return
    }
    if count == 0 {
        // Connection closed by client
        fmt.Printf("Connection closed by client: fd %d\n", fd)
        closeClientConnection(epollFd, fd)
        return
    }
    fmt.Printf("Read %d bytes from fd %d: %s\n", count, fd, string(buffer[:count]))
    // Echo the received data back to the client
    if _, err := unix.Write(fd, buffer[:count]); err != nil {
        fmt.Printf("Write error: %v\n", err)
        closeClientConnection(epollFd, fd)
    }
}

func closeClientConnection(epollFd int, fd int) {
    // Remove the file descriptor from epoll
    if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_DEL, fd, nil); err != nil {
        fmt.Printf("Failed to remove file descriptor from epoll: %v\n", err)
    }
    // Close the file descriptor
    if err := unix.Close(fd); err != nil {
        fmt.Printf("Failed to close file descriptor: %v\n", err)
    }
    fmt.Printf("Closed client connection: fd %d\n", fd)
}
