package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/input"
)

// Fixes against the original unix_sock.go (2026-06-09 build):
//  1. FLBPluginInputCallback drained only ONE record per collector tick (1s)
//     -> hard cap of 1 record/s per instance, everything else piled up in
//     blocked handleConn goroutines and was lost on instance rotation.
//     Now the callback drains the whole queue (bounded by maxFlushBytes).
//  2. bufio.Scanner (ScanLines, 64KB max token) was used on a stream that has
//     no newlines (fluent-logger-php JsonPacker) -> "token too long" dropped a
//     whole connection's records. Now json.Decoder parses concatenated JSON
//     values directly, with no size limit.
//  3. Event time now comes from the forward-protocol time field (arr[1])
//     instead of time.Now(), so backlog does not shift timestamps.
//  4. Per-record fmt.Println removed (journal noise).
//  5. FLBPluginInputCleanupCallback added so the buffer handed to C is freed
//     (requires fluent-bit >= 2.0).

const (
	queueSize     = 65536           // records buffered between conns and collector
	maxFlushBytes = 4 * 1024 * 1024 // max msgpack bytes returned per collector tick
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
// LogRecord
// =========================

type LogRecord struct {
	TableName    string                 `json:"table_name"`
	TableVersion string                 `json:"table_version"`
	Record       map[string]interface{} `json:"record"`
}

// record may be a JSON object or a string containing JSON
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

	if a.Record[0] == '"' {
		var s string
		if err := json.Unmarshal(a.Record, &s); err != nil {
			return err
		}
		return json.Unmarshal([]byte(s), &d.Record)
	}

	return json.Unmarshal(a.Record, &d.Record)
}

// =========================
// Plugin registration
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

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Println("create socket dir error:", err)
		return input.FLB_ERROR
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
		queue:      make(chan []byte, queueSize),
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
// connection handling
// =========================

func handleConn(c *UnixSocketContext, conn *net.UnixConn) {
	defer conn.Close()
	defer c.wg.Done()

	// json.Decoder consumes concatenated JSON values ("[..][..]" with no
	// delimiter) and has no fixed token size limit.
	dec := json.NewDecoder(conn)

	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err != io.EOF {
				fmt.Println("decode error:", err)
			}
			return
		}

		if !enqueue(c, raw) {
			return
		}
	}
}

// parseEventTime converts the forward-protocol time field (int/float seconds)
// into time.Time; falls back to now.
func parseEventTime(v interface{}) time.Time {
	switch t := v.(type) {
	case float64:
		if t > 0 {
			sec := int64(t)
			nsec := int64((t - float64(sec)) * 1e9)
			return time.Unix(sec, nsec)
		}
	case json.Number:
		if f, err := t.Float64(); err == nil && f > 0 {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			return time.Unix(sec, nsec)
		}
	}
	return time.Now()
}

// enqueue parses one JSON value, flattens it and pushes it to the queue.
// Returns false when the plugin is stopping.
func enqueue(c *UnixSocketContext, raw json.RawMessage) bool {
	var record LogRecord
	eventTime := time.Now()

	// 1) forward protocol: ["tag", time, {record}]
	var arr []interface{}
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) == 3 {
		eventTime = parseEventTime(arr[1])

		payload, _ := json.Marshal(arr[2])
		if err := json.Unmarshal(payload, &record); err != nil {
			record = LogRecord{
				TableName:    "unknown",
				TableVersion: "v1",
				Record:       map[string]interface{}{"message": fmt.Sprintf("%v", arr[2])},
			}
		}
	} else {
		// 2) plain JSON / LogRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			record = LogRecord{
				TableName:    "unknown",
				TableVersion: "v1",
				Record:       map[string]interface{}{"message": string(raw)},
			}
		}
	}

	// 3) flatten
	out := make(map[string]interface{})
	out["record"] = record.Record
	if record.TableName != "" {
		out["table_name"] = record.TableName
	}
	if record.TableVersion != "" {
		out["table_version"] = record.TableVersion
	}

	entry := []interface{}{input.FLBTime{Time: eventTime}, out}

	enc := input.NewEncoder()
	packed, err := enc.Encode(entry)
	if err != nil {
		fmt.Println("encode error:", err)
		return true
	}

	select {
	case c.queue <- packed:
	case <-c.stop:
		return false
	}
	return true
}

// =========================
// Fluent Bit callbacks
// =========================

//export FLBPluginInputCallback
func FLBPluginInputCallback(data *unsafe.Pointer, size *C.size_t) int {
	var buf []byte

drain:
	for len(buf) < maxFlushBytes {
		select {
		case msg := <-ctx.queue:
			buf = append(buf, msg...)
		default:
			break drain
		}
	}

	if len(buf) == 0 {
		return input.FLB_OK
	}

	*data = C.CBytes(buf)
	*size = C.size_t(len(buf))
	return input.FLB_OK
}

//export FLBPluginInputCleanupCallback
func FLBPluginInputCleanupCallback(data unsafe.Pointer) int {
	C.free(data)
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
