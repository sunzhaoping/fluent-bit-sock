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

		// 先尝试解析成 Forward Protocol 数组
		var arr []interface{}
		var record map[string]interface{}
		tag := "default"

		if err := json.Unmarshal([]byte(line), &arr); err == nil && len(arr) == 3 {
			// 第一项是 tag
			if t, ok := arr[0].(string); ok {
				tag = t
			}

			// 第三项是 record
			if r, ok := arr[2].(map[string]interface{}); ok {
				record = r
			} else {
				// 第三项不是 map，就用字符串包装
				record = map[string]interface{}{
					"message": fmt.Sprintf("%v", arr[2]),
				}
			}
		} else {
			// 不是 Forward Protocol 数组，尝试解析成普通 JSON
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				// 都解析不了，就直接用 message 字段
				record = map[string]interface{}{
					"message": line,
				}
			}
		}

		// 构造 entry
		entry := []interface{}{flbTime, record}

		enc := input.NewEncoder()
		packed, err := enc.Encode(entry)
		if err != nil {
			fmt.Println("[gunixsocket] encode error:", err)
			continue
		}

		// 发送到 queue
		select {
		case c.queue <- packed:
		case <-c.stop:
			fmt.Println("[gunixsocket] stop signal received")
			return
		}

		// 打印 tag （可选，用于调试）
		fmt.Println("[gunixsocket] tag:", tag)
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
