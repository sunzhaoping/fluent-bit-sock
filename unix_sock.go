package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/input"
)

type UnixSocketContext struct {
	listener *net.UnixListener
	queue    chan []byte
	stop     chan struct{}
	wg       sync.WaitGroup
}

var ctx *UnixSocketContext

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return input.FLBPluginRegister(def, "gunixsocket", "Unix Socket Text Input Plugin with TCP/UDP & perm")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// 获取配置
	path := input.FLBPluginConfigKey(plugin, "Path")
	if path == "" {
		path = "/tmp/fluent.sock"
	}

	mode := input.FLBPluginConfigKey(plugin, "Mode")
	if mode == "" {
		mode = "tcp"
	}

	permStr := input.FLBPluginConfigKey(plugin, "Perm")
	if permStr == "" {
		permStr = "0644"
	}
	perm, err := strconv.ParseUint(permStr, 8, 32)
	if err != nil {
		fmt.Println("[gunixsocket] invalid perm, using 0644")
		perm = 0644
	}

	fmt.Println("[gunixsocket] path:", path, "mode:", mode, "perm:", fmt.Sprintf("%04o", perm))

	// 删除已存在 socket 文件
	os.Remove(path)

	if mode != "tcp" && mode != "udp" {
		fmt.Println("[gunixsocket] unsupported mode, must be tcp or udp")
		return input.FLB_ERROR
	}

	// 仅 TCP 模式需要监听 UnixSocket
	if mode == "tcp" {
		addr, err := net.ResolveUnixAddr("unix", path)
		if err != nil {
			fmt.Println("[gunixsocket] resolve addr error:", err)
			return input.FLB_ERROR
		}

		listener, err := net.ListenUnix("unix", addr)
		if err != nil {
			fmt.Println("[gunixsocket] listen error:", err)
			return input.FLB_ERROR
		}

		// 设置文件权限
		if err := os.Chmod(path, os.FileMode(perm)); err != nil {
			fmt.Println("[gunixsocket] chmod error:", err)
		}

		ctx = &UnixSocketContext{
			listener: listener,
			queue:    make(chan []byte, 4096),
			stop:     make(chan struct{}),
		}

		ctx.wg.Add(1)
		go acceptLoop(ctx)
	} else if mode == "udp" {
		// UDP 模式
		addr, err := net.ResolveUnixAddr("unixgram", path)
		if err != nil {
			fmt.Println("[gunixsocket] resolve udp addr error:", err)
			return input.FLB_ERROR
		}

		conn, err := net.ListenUnixgram("unixgram", addr)
		if err != nil {
			fmt.Println("[gunixsocket] listen udp error:", err)
			return input.FLB_ERROR
		}

		// 设置权限
		if err := os.Chmod(path, os.FileMode(perm)); err != nil {
			fmt.Println("[gunixsocket] chmod error:", err)
		}

		ctx = &UnixSocketContext{
			listener: nil, // TCP listener 不用
			queue:    make(chan []byte, 4096),
			stop:     make(chan struct{}),
		}

		ctx.wg.Add(1)
		go udpLoop(conn)
	}

	return input.FLB_OK
}

// TCP Accept 循环
func acceptLoop(c *UnixSocketContext) {
	defer c.wg.Done()
	for {
		conn, err := c.listener.AcceptUnix()
		if err != nil {
			select {
			case <-c.stop:
				return
			default:
				fmt.Println("[gunixsocket] accept error:", err)
				continue
			}
		}
		c.wg.Add(1)
		go handleConn(c, conn)
	}
}

// 处理 TCP 单连接
func handleConn(c *UnixSocketContext, conn *net.UnixConn) {
	defer conn.Close()
	defer c.wg.Done()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		now := time.Now()
		flbTime := input.FLBTime{Time: now}
		record := map[string]string{"log": line}
		entry := []interface{}{flbTime, record}
		enc := input.NewEncoder()
		packed, err := enc.Encode(entry)
		if err != nil {
			fmt.Println("[gunixsocket] encode error:", err)
			continue
		}
		select {
		case c.queue <- packed:
		case <-c.stop:
			return
		}
	}
}

// UDP 接收循环
func udpLoop(conn *net.UnixConn) {
	defer ctx.wg.Done()
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.stop:
			return
		default:
		}
		n, _, err := conn.ReadFromUnix(buf)
		if err != nil {
			continue
		}
		line := string(buf[:n])
		now := time.Now()
		flbTime := input.FLBTime{Time: now}
		record := map[string]string{"log": line}
		entry := []interface{}{flbTime, record}
		enc := input.NewEncoder()
		packed, err := enc.Encode(entry)
		if err != nil {
			continue
		}
		select {
		case ctx.queue <- packed:
		case <-ctx.stop:
			return
		}
	}
}

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	select {
	case msg := <-ctx.queue:
		*data = C.CBytes(msg)
		*size = C.size_t(len(msg))
	default:
		return input.FLB_OK
	}
	return input.FLB_OK
}

//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	close(ctx.stop)
	if ctx.listener != nil {
		ctx.listener.Close()
	}
	ctx.wg.Wait()
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return input.FLB_OK
}

func main() {}
