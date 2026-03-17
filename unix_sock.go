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
	"strings"
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

// =========================
// LogRecord（核心）
// =========================

type LogRecord struct {
	TableName    string                 `json:"table_name"`
	TableVersion string                 `json:"table_version"`
	Record       map[string]interface{} `json:"record"`
}

// 支持 record = JSON 或 string(JSON)
func (d *LogRecord) UnmarshalJSON(data []byte) error {
	type Alias struct {
		TableName    string          `json:"table_name"`
		TableVersion string          `json:"table_version"`
		Record       json.RawMessage `json:"record"`
	}

	var a Alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}

	d.TableName = a.TableName
	d.TableVersion = a.TableVersion

	if len(a.Record) == 0 {
		d.Record = make(map[string]interface{})
		return nil
	}

	// 👇 string 包 JSON
	if a.Record[0] == '"' {
		var s string
		if err := json.Unmarshal(a.Record, &s); err != nil {
			return err
		}
		return json.Unmarshal([]byte(s), &d.Record)
	}

	// 👇 正常 JSON
	return json.Unmarshal(a.Record, &d.Record)
}

// =========================
// Plugin 注册
// =========================

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

	if _, err := os.Stat(path); err == nil {
		os.Remove(path)
	}

	permStr := input.FLBPluginConfigKey(plugin, "Perm")
	if permStr == "" {
		permStr = "0644"
	}

	perm, err := strconv.ParseUint(permStr, 8, 32)
	if err != nil {
		perm = 0644
	}

	addr, err := net.ResolveUnixAddr("unix", path)
	if err != nil {
		fmt.Println("resolve error:", err)
		return input.FLB_ERROR
	}

	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		fmt.Println("listen error:", err)
		return input.FLB_ERROR
	}

	os.Chmod(path, os.FileMode(perm))

	ctx = &UnixSocketContext{
		listener:   listener,
		queue:      make(chan []byte, 4096),
		stop:       make(chan struct{}),
		socketPath: path,
	}

	ctx.wg.Add(1)
	go acceptLoop(ctx)

	return input.FLB_OK
}

// =========================
// accept loop
// =========================

func acceptLoop(c *UnixSocketContext) {
	defer c.wg.Done()

	for {
		conn, err := c.listener.AcceptUnix()
		if err != nil {
			select {
			case <-c.stop:
				return
			default:
				fmt.Println("accept error:", err)
				continue
			}
		}

		c.wg.Add(1)
		go handleConn(c, conn)
	}
}

// =========================
// 核心处理逻辑
// =========================

func handleConn(c *UnixSocketContext, conn *net.UnixConn) {
	defer conn.Close()
	defer c.wg.Done()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		now := time.Now()
		flbTime := input.FLBTime{Time: now}

		line := scanner.Text()
		line = strings.ReplaceAll(line, "][", "]\n[")

		for _, subline := range strings.Split(line, "\n") {
			subline = strings.TrimSpace(subline)
			if subline == "" {
				continue
			}

			var record LogRecord
			tag := "default"

			// =========================
			// 1️⃣ Forward Protocol
			// =========================
			var arr []interface{}
			if err := json.Unmarshal([]byte(subline), &arr); err == nil && len(arr) == 3 {

				if t, ok := arr[0].(string); ok {
					tag = t
				}

				raw, _ := json.Marshal(arr[2])

				if err := json.Unmarshal(raw, &record); err != nil {
					record = LogRecord{
						TableName:    "unknown",
						TableVersion: "v1",
						Record:       map[string]interface{}{"message": fmt.Sprintf("%v", arr[2])},
					}
				}

			} else {
				// =========================
				// 2️⃣ 普通 JSON / LogRecord
				// =========================
				if err := json.Unmarshal([]byte(subline), &record); err != nil {
					record = LogRecord{
						TableName:    "unknown",
						TableVersion: "v1",
						Record:       map[string]interface{}{"message": subline},
					}
				}
			}

			// =========================
			// 3️⃣ flatten 输出
			// =========================
			out := make(map[string]interface{})

			out["record"] = record.Record

			if record.TableName != "" {
				out["table_name"] = record.TableName
				tag = record.TableName // 👈 自动当 tag
			}

			if record.TableVersion != "" {
				out["table_version"] = record.TableVersion
			}

			entry := []interface{}{flbTime, out}

			enc := input.NewEncoder()
			packed, err := enc.Encode(entry)
			if err != nil {
				fmt.Println("encode error:", err)
				continue
			}

			select {
			case c.queue <- packed:
			case <-c.stop:
				return
			}

			fmt.Println("[gunixsocket] tag:", tag)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("scanner error:", err)
	}
}

// =========================
// Fluent Bit 回调
// =========================

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	select {
	case msg := <-ctx.queue:
		*data = C.CBytes(msg)
		*size = C.size_t(len(msg))
	default:
	}
	return input.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	close(ctx.stop)
	ctx.listener.Close()
	ctx.wg.Wait()

	if ctx.removeSock {
		os.Remove(ctx.socketPath)
	}

	return input.FLB_OK
}

func main() {}
