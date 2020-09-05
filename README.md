# go-redis extensions

## Tracing using OpenTelemetryHook

For more details see [documentation](https://redis.uptrace.dev/tracing/):

```go
rdb := redis.NewClient(&redis.Options{...})
rdb.AddHook(&redisext.OpenTelemetryHook{})
```
