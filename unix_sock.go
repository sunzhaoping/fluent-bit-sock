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

// UnixSocketContext 保存插件状态
type UnixSocketContext struct {
	listener *net.UnixListener
	queue    chan []byte
	stop     chan struct{}
	wg       sync.WaitGroup
}

var ctx *UnixSocketContext

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return input.FLBPluginRegister(def, "gunixsocket", "Unix Socket Text Input Plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	path := input.FLBPluginConfigKey(plugin, "Path")
	if path == "" {
		path = "/tmp/fluent.sock"
	}

	fmt.Println("[gunixsocket] socket path:", path)
	os.Remove(path)

	permStr := input.FLBPluginConfigKey(plugin, "Perm")
	if permStr == "" {
		permStr = "0644"
	}
	perm, err := strconv.ParseUint(permStr, 8, 32)
	if err != nil {
		fmt.Println("[gunixsocket] invalid perm, using 0644")
		perm = 0644
	}

	fmt.Println("[gunixsocket] path:", path, "perm:", fmt.Sprintf("%04o", perm))

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

	if err := os.Chmod(path, os.FileMode(perm)); err != nil {
		fmt.Println("[gunixsocket] chmod error:", err)
	}

	ctx = &UnixSocketContext{
		listener: listener,
		queue:    make(chan []byte, 4096), // 缓存队列
		stop:     make(chan struct{}),
	}

	ctx.wg.Add(1)
	go acceptLoop(ctx)

	return input.FLB_OK
}

// 接收连接循环
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

// 处理单个连接
func handleConn(c *UnixSocketContext, conn *net.UnixConn) {
	defer conn.Close()
	defer c.wg.Done()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		now := time.Now()
		flbTime := input.FLBTime{Time: now}

		record := map[string]string{
			"log": line,
		}
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

	if err := scanner.Err(); err != nil {
		fmt.Println("[gunixsocket] scanner error:", err)
	}
}

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	select {
	case msg := <-ctx.queue:
		*data = C.CBytes(msg)
		*size = C.size_t(len(msg))
	default:
		// 没有数据
		return input.FLB_OK
	}
	return input.FLB_OK
}

//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	close(ctx.stop)
	ctx.listener.Close()
	ctx.wg.Wait()
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return input.FLB_OK
}

func main() {}
