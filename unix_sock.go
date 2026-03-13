package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"bufio"
	"encoding/json"
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
	listener   *net.UnixListener
	queue      chan []byte
	stop       chan struct{}
	wg         sync.WaitGroup
	socketPath string
	removeSock bool
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

	// 只在文件存在时删除
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			fmt.Println("[gunixsocket] remove existing socket failed:", err)
		}
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
		listener:   listener,
		queue:      make(chan []byte, 4096),
		stop:       make(chan struct{}),
		socketPath: path,
		removeSock: false, // 退出时是否删除 socket
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
				fmt.Println("[gunixsocket] accept stopped")
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

		// 试着解析成 JSON，如果不是 JSON，就直接用 string
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			// 不是 JSON 的话，用字符串也可以
			record = map[string]interface{}{
				"message": line,
			}
		}

		// 直接把 record 放到 entry，去掉 "log" 包装
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
			fmt.Println("[gunixsocket] stop signal received")
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
		return input.FLB_OK
	default:
		return input.FLB_OK
	}
}

//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	fmt.Println("[gunixsocket] FLBPluginInputCleanupCallback")
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	fmt.Println("[gunixsocket] FLBPluginExit")

	close(ctx.stop)
	ctx.listener.Close()
	ctx.wg.Wait()

	if ctx.removeSock {
		if err := os.Remove(ctx.socketPath); err != nil {
			fmt.Println("[gunixsocket] remove socket failed:", err)
		}
	}

	return input.FLB_OK
}

func main() {}
