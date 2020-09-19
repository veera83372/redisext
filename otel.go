package redisext

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redisext/internal"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
)

var tracer = global.Tracer("github.com/go-redis/redis")

type OpenTelemetryHook struct{}

var _ redis.Hook = OpenTelemetryHook{}

func (OpenTelemetryHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	b := make([]byte, 0, 32)
	b = appendCmd(b, cmd)

	ctx, span := tracer.Start(ctx, cmd.FullName())
	span.SetAttributes(
		label.String("db.system", "redis"),
		label.String("redis.cmd", internal.String(b)),
	)

	return ctx, nil
}

func (OpenTelemetryHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmd.Err(); err != nil {
		recordError(ctx, span, err)
	}
	span.End()
	return nil
}

func (OpenTelemetryHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	const numCmdLimit = 100
	const numNameLimit = 10

	seen := make(map[string]struct{}, len(cmds))
	unqNames := make([]string, 0, len(cmds))

	b := make([]byte, 0, 32*len(cmds))

	for i, cmd := range cmds {
		if i > numCmdLimit {
			break
		}

		if i > 0 {
			b = append(b, '\n')
		}
		b = appendCmd(b, cmd)

		if len(unqNames) >= numNameLimit {
			continue
		}

		name := cmd.FullName()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			unqNames = append(unqNames, name)
		}
	}

	tracer := global.Tracer("github.com/go-redis/redis")
	ctx, span := tracer.Start(ctx, "pipeline "+strings.Join(unqNames, " "))
	span.SetAttributes(
		label.String("db.system", "redis"),
		label.Int("redis.num_cmd", len(cmds)),
		label.String("redis.cmds", internal.String(b)),
	)

	return ctx, nil
}

func (OpenTelemetryHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmds[0].Err(); err != nil {
		recordError(ctx, span, err)
	}
	span.End()
	return nil
}

func recordError(ctx context.Context, span trace.Span, err error) {
	if err != redis.Nil {
		span.RecordError(ctx, err)
	}
}

func appendCmd(b []byte, cmd redis.Cmder) []byte {
	const lenLimit = 64

	for i, arg := range cmd.Args() {
		if i > 0 {
			b = append(b, ' ')
		}

		start := len(b)
		b = appendArg(b, arg)
		if len(b)-start > lenLimit {
			b = append(b[:start+lenLimit], "..."...)
		}
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	}

	return b
}

func appendArg(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		return appendUTF8String(b, internal.Bytes(v))
	case []byte:
		return appendUTF8String(b, v)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case int8:
		return strconv.AppendInt(b, int64(v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case uint:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case float32:
		return strconv.AppendFloat(b, float64(v), 'f', -1, 64)
	case float64:
		return strconv.AppendFloat(b, v, 'f', -1, 64)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case time.Time:
		return v.AppendFormat(b, time.RFC3339Nano)
	default:
		return append(b, fmt.Sprint(v)...)
	}
}

func appendUTF8String(dst []byte, src []byte) []byte {
	if isSimple(src) {
		dst = append(dst, src...)
		return dst
	}

	s := len(dst)
	dst = append(dst, make([]byte, hex.EncodedLen(len(src)))...)
	hex.Encode(dst[s:], src)
	return dst
}

func isSimple(b []byte) bool {
	for _, c := range b {
		if !isSimpleByte(c) {
			return false
		}
	}
	return true
}

func isSimpleByte(c byte) bool {
	return simple[c]
}

var simple = [256]bool{
	'-': true,
	'_': true,

	'0': true,
	'1': true,
	'2': true,
	'3': true,
	'4': true,
	'5': true,
	'6': true,
	'7': true,
	'8': true,
	'9': true,

	'a': true,
	'b': true,
	'c': true,
	'd': true,
	'e': true,
	'f': true,
	'g': true,
	'h': true,
	'i': true,
	'j': true,
	'k': true,
	'l': true,
	'm': true,
	'n': true,
	'o': true,
	'p': true,
	'q': true,
	'r': true,
	's': true,
	't': true,
	'u': true,
	'v': true,
	'w': true,
	'x': true,
	'y': true,
	'z': true,

	'A': true,
	'B': true,
	'C': true,
	'D': true,
	'E': true,
	'F': true,
	'G': true,
	'H': true,
	'I': true,
	'J': true,
	'K': true,
	'L': true,
	'M': true,
	'N': true,
	'O': true,
	'P': true,
	'Q': true,
	'R': true,
	'S': true,
	'T': true,
	'U': true,
	'V': true,
	'W': true,
	'X': true,
	'Y': true,
	'Z': true,
}
