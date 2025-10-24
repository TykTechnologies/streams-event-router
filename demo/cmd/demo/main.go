package main

import (
    "bytes"
    "context"
    "crypto/tls"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    kafka "github.com/segmentio/kafka-go"
    amqp "github.com/Azure/go-amqp"
    eventpb "github.com/example/event-router-demo/internal/pb/event"
    "google.golang.org/protobuf/encoding/protojson"
    pbproto "google.golang.org/protobuf/proto"
)

type SSEEvent struct {
    EventType string
    Data      string
}

var (
    kafkaUp        bool
    amqpUp         bool
    amqpExternalUp bool
    bentoHTTPUp    bool
    statusMu       sync.Mutex

    sseClients   []chan SSEEvent
    sseClientsMu sync.Mutex
)

func env(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

func main() {
    kafkaBrokerAddress := env("KAFKA_BROKER", "127.0.0.1:9092")
    kafkaTopic := env("KAFKA_TOPIC", "high-priority-topic")
    kafkaGroupID := env("KAFKA_GROUP", "demo-group")

    amqpBrokerAddress := env("AMQP_ADDR", "amqp://localhost:5672")
    amqpSourceAddress := env("AMQP_SOURCE", "low-priority-queue")
    amqpTargetAddress := env("AMQP_TARGET", "inbound-queue")
    bentoURL := env("BENTO_URL", "http://streams:4195/inbound")

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

    go consumeKafka(ctx, kafkaBrokerAddress, kafkaTopic, kafkaGroupID)
    go consumeAMQP(ctx, amqpBrokerAddress, amqpSourceAddress)
    // Manual-only demo: no automated AMQP tick sender
    go monitorGatewayHealth(ctx, kafkaBrokerAddress, amqpBrokerAddress, amqpTargetAddress, bentoURL)

    http.HandleFunc("/", serveIndex)
    http.HandleFunc("/sse", serveSSE)
    // Emit endpoints -> send events to Bento HTTP input
    http.HandleFunc("/emit/order-created", func(w http.ResponseWriter, r *http.Request) {
        sendCE(w, bentoURL, "ORDER_CREATED", map[string]any{"orderId": time.Now().UnixNano()})
    })
    http.HandleFunc("/emit/user-registered", func(w http.ResponseWriter, r *http.Request) {
        sendCE(w, bentoURL, "USER_REGISTERED", map[string]any{"userId": time.Now().UnixNano()})
    })
    http.HandleFunc("/emit/audit", func(w http.ResponseWriter, r *http.Request) {
        sendCE(w, bentoURL, "AUDIT_LOG", map[string]any{"action": "demo-click", "ts": time.Now().Format(time.RFC3339)})
    })
    http.HandleFunc("/emit/broadcast", func(w http.ResponseWriter, r *http.Request) {
        sendCE(w, bentoURL, "BROADCAST_DEMO", map[string]any{"note": "fanout"})
    })
    // Manual AMQP inbound (send one message directly to inbound-queue)
    http.HandleFunc("/emit/inbound-amqp", func(w http.ResponseWriter, r *http.Request) {
        if err := sendOneInboundAMQP(amqpBrokerAddress, amqpTargetAddress); err != nil {
            w.WriteHeader(http.StatusBadGateway)
            _, _ = w.Write([]byte(err.Error()))
            return
        }
        w.WriteHeader(http.StatusAccepted)
    })
    http.HandleFunc("/emit/person", func(w http.ResponseWriter, r *http.Request) {
        body := map[string]any{"firstName": "caleb", "lastName": "quaye", "email": "caleb@myspace.com"}
        postJSON(w, bentoURL, body)
    })
    http.HandleFunc("/emit/raw", func(w http.ResponseWriter, r *http.Request) {
        var m map[string]any
        if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
            w.WriteHeader(http.StatusBadRequest); _, _ = w.Write([]byte("invalid json")); return
        }
        postJSON(w, bentoURL, m)
    })
    http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
        // Simple collector endpoint used by Bento default HTTP output
        b := mustReadAllLimit(r.Body, 1<<20)
        broadcastEvent(SSEEvent{EventType: "outputHttpMessage", Data: fmt.Sprintf("HTTP received: %s", string(b))})
        w.WriteHeader(http.StatusAccepted)
    })

    srv := &http.Server{Addr: ":8080"}
    go func() {
        log.Println("Demo UI: http://localhost:8080")
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Printf("HTTP server error: %v", err)
            cancel()
        }
    }()

    <-sigCh
    log.Println("Shutting down...")
    cancel()
    ctx2, c2 := context.WithTimeout(context.Background(), 5*time.Second)
    defer c2()
    _ = srv.Shutdown(ctx2)
}

// ---- Kafka ----
func consumeKafka(ctx context.Context, broker, topic, group string) {
    for {
        if ctx.Err() != nil { return }
        r := kafka.NewReader(kafka.ReaderConfig{
            Brokers:  []string{broker},
            GroupID:  group,
            Topic:    topic,
            MinBytes: 1,
            MaxBytes: 10e6,
        })
        if err := testKafkaConnection(ctx, broker); err != nil {
            setKafkaStatus(false)
            _ = r.Close()
            if !sleepOrExit(ctx, 2*time.Second) { return }
            continue
        }
        setKafkaStatus(true)
        for {
            m, err := r.ReadMessage(ctx)
            if err != nil { _ = r.Close(); setKafkaStatus(false); break }
            // Try to decode event.Event protobuf
            var ev eventpb.Event
            if err := pbproto.Unmarshal(m.Value, &ev); err == nil {
                j, _ := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(&ev)
                broadcastEvent(SSEEvent{EventType: "outputKafkaMessage", Data: fmt.Sprintf("Kafka event (decoded): %s", string(j))})
            } else {
                broadcastEvent(SSEEvent{EventType: "outputKafkaMessage", Data: fmt.Sprintf("Kafka bytes (%dB)", len(m.Value))})
            }
        }
        if !sleepOrExit(ctx, 2*time.Second) { return }
    }
}

func testKafkaConnection(ctx context.Context, broker string) error {
    d := &kafka.Dialer{Timeout: time.Second, DualStack: true}
    c, err := d.DialContext(ctx, "tcp", broker)
    if err != nil { return err }
    defer c.Close()
    _, err = c.Brokers()
    return err
}

// ---- AMQP 1.0 ----
func consumeAMQP(ctx context.Context, addr, source string) {
    for {
        if ctx.Err() != nil { return }
        dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
        conn, err := amqp.Dial(dialCtx, addr, &amqp.ConnOptions{
            SASLType: amqp.SASLTypePlain("guest","guest"),
            TLSConfig: &tls.Config{InsecureSkipVerify: true},
        })
        cancel()
        if err != nil { setAMQPStatus(false); if !sleepOrExit(ctx, 2*time.Second) { return }; continue }
        session, err := conn.NewSession(ctx, nil)
        if err != nil { _ = conn.Close(); setAMQPStatus(false); if !sleepOrExit(ctx, 2*time.Second) { return }; continue }
        recv, err := session.NewReceiver(ctx, source, nil)
        if err != nil { _ = session.Close(ctx); _ = conn.Close(); setAMQPStatus(false); if !sleepOrExit(ctx, 2*time.Second) { return }; continue }
        setAMQPStatus(true)
        for {
            msg, err := recv.Receive(ctx, nil)
            if err != nil { _ = recv.Close(ctx); _ = session.Close(ctx); _ = conn.Close(); setAMQPStatus(false); break }
            _ = recv.AcceptMessage(ctx, msg)
            broadcastEvent(SSEEvent{EventType: "outputAmqpMessage", Data: fmt.Sprintf("AMQP says: %s", string(msg.GetData()))})
        }
    }
}

// sendOneInboundAMQP sends a single CloudEvents-like JSON to the inbound AMQP target
func sendOneInboundAMQP(addr, target string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    conn, err := amqp.Dial(ctx, addr, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest","guest"), TLSConfig: &tls.Config{InsecureSkipVerify: true}})
    if err != nil { return err }
    defer conn.Close()
    sess, err := conn.NewSession(ctx, nil); if err != nil { return err }
    defer sess.Close(ctx)
    snd, err := sess.NewSender(ctx, target, nil); if err != nil { return err }
    defer snd.Close(ctx)
    body := map[string]any{
        "id": time.Now().UnixNano(),
        "source": "demo/manual-amqp",
        "type": "USER_REGISTERED",
        "specversion": "1.0",
        "data": map[string]any{"note": "manual inbound"},
    }
    payload, _ := json.Marshal(body)
    return snd.Send(ctx, amqp.NewMessage(payload), nil)
}

// ---- Health ----
func monitorGatewayHealth(ctx context.Context, broker, amqpAddr, amqpTarget, bentoURL string) {
    ticker := time.NewTicker(5*time.Second); defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            setKafkaStatus(checkKafka(broker))
            setAMQPStatus(checkAMQP(amqpAddr))
            setExternalAMQPStatus(checkExternalAMQP(amqpAddr, amqpTarget))
            setBentoHTTPStatus(checkBentoHTTP(bentoURL))
        }
    }
}

func checkKafka(broker string) bool { return testKafkaConnection(context.Background(), broker) == nil }
func checkAMQP(addr string) bool {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel()
    c, err := amqp.Dial(ctx, addr, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest","guest"), TLSConfig: &tls.Config{InsecureSkipVerify: true}})
    if err != nil { return false }
    defer c.Close(); s, err := c.NewSession(ctx, nil); if err != nil { return false }
    _ = s.Close(ctx); return true
}
func checkExternalAMQP(addr, target string) bool {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel()
    c, err := amqp.Dial(ctx, addr, &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("guest","guest"), TLSConfig: &tls.Config{InsecureSkipVerify: true}})
    if err != nil { return false }
    defer c.Close(); s, err := c.NewSession(ctx, nil); if err != nil { return false }
    defer s.Close(ctx); snd, err := s.NewSender(ctx, target, nil); if err != nil { return false }
    _ = snd.Close(ctx); return true
}

func setKafkaStatus(up bool) { statusMu.Lock(); ch := (kafkaUp != up); kafkaUp = up; statusMu.Unlock(); if ch { broadcastEvent(SSEEvent{"outputKafkaStatus", boolToStatus(up)}) } }
func setAMQPStatus(up bool) { statusMu.Lock(); ch := (amqpUp != up); amqpUp = up; statusMu.Unlock(); if ch { broadcastEvent(SSEEvent{"outputAmqpStatus", boolToStatus(up)}) } }
func setExternalAMQPStatus(up bool) { statusMu.Lock(); ch := (amqpExternalUp != up); amqpExternalUp = up; statusMu.Unlock(); if ch { broadcastEvent(SSEEvent{"inputAmqpStatus", boolToStatus(up)}) } }
func setBentoHTTPStatus(up bool) { statusMu.Lock(); ch := (bentoHTTPUp != up); bentoHTTPUp = up; statusMu.Unlock(); if ch { broadcastEvent(SSEEvent{"inputHttpStatus", boolToStatus(up)}) } }

func boolToStatus(up bool) string { if up { return "UP" }; return "DOWN" }

// ---- SSE + Helpers ----
func broadcastEvent(ev SSEEvent) {
    sseClientsMu.Lock(); defer sseClientsMu.Unlock()
    for _, ch := range sseClients { select { case ch <- ev: default: } }
}

func serveSSE(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    clientCh := make(chan SSEEvent, 10)
    sseClientsMu.Lock(); sseClients = append(sseClients, clientCh); sseClientsMu.Unlock()
    defer func() {
        sseClientsMu.Lock(); for i, ch := range sseClients { if ch == clientCh { sseClients = append(sseClients[:i], sseClients[i+1:]...); break } }; sseClientsMu.Unlock(); close(clientCh)
    }()
    statusMu.Lock(); ks, as, es, bs := boolToStatus(kafkaUp), boolToStatus(amqpUp), boolToStatus(amqpExternalUp), boolToStatus(bentoHTTPUp); statusMu.Unlock()
    fmt.Fprintf(w, "event: outputKafkaStatus\ndata: %s\n\n", ks)
    fmt.Fprintf(w, "event: outputAmqpStatus\ndata: %s\n\n", as)
    fmt.Fprintf(w, "event: inputAmqpStatus\ndata: %s\n\n", es)
    fmt.Fprintf(w, "event: inputHttpStatus\ndata: %s\n\n", bs)
    if f, ok := w.(http.Flusher); ok { f.Flush() }
    for {
        select {
        case ev := <-clientCh:
            fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.EventType, ev.Data)
            if f, ok := w.(http.Flusher); ok { f.Flush() }
        case <-r.Context().Done():
            return
        }
    }
}

func serveIndex(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    fmt.Fprint(w, indexHTML)
}

func sleepOrExit(ctx context.Context, d time.Duration) bool { select { case <-time.After(d): return true; case <-ctx.Done(): return false } }

func mustReadAllLimit(rc interface{ Read([]byte)(int,error); Close() error }, limit int64) []byte {
    defer rc.Close()
    var buf bytes.Buffer
    tmp := make([]byte, 4096)
    var total int64
    for {
        n, err := rc.Read(tmp)
        if n > 0 {
            total += int64(n)
            if total > limit { break }
            _, _ = buf.Write(tmp[:n])
        }
        if err != nil { break }
    }
    return buf.Bytes()
}

func sendCE(w http.ResponseWriter, bentoURL, typ string, data map[string]any) {
    body := map[string]any{
        "id": time.Now().UnixNano(),
        "source": "demo/ui",
        "type": typ,
        "specversion": "1.0",
        "data": data,
    }
    postJSON(w, bentoURL, body)
}

func postJSON(w http.ResponseWriter, url string, body any) {
    b, _ := json.Marshal(body)
    candidates := []string{
        url,
        "http://streams:4195/inbound",
        "http://host.docker.internal:4195/inbound",
        "http://localhost:4195/inbound",
    }
    var lastErr error
    // Allow for pipeline latency (LOW path sleeps 5s), so use a generous timeout.
    client := &http.Client{Timeout: 12 * time.Second}
    for _, u := range candidates {
        req, _ := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
        req.Header.Set("Content-Type", "application/json")
        resp, err := client.Do(req)
        if err != nil {
            lastErr = err
            continue
        }
        defer resp.Body.Close()
        w.WriteHeader(resp.StatusCode)
        return
    }
    w.WriteHeader(http.StatusBadGateway)
    if lastErr != nil { _, _ = w.Write([]byte(lastErr.Error())) }
}

// Simple readiness check for Bento HTTP input
func checkBentoHTTP(url string) bool {
    candidates := []string{url, "http://streams:4195/inbound", "http://host.docker.internal:4195/inbound", "http://localhost:4195/inbound"}
    client := &http.Client{Timeout: 1500 * time.Millisecond}
    for _, u := range candidates {
        req, _ := http.NewRequest(http.MethodOptions, u, nil)
        resp, err := client.Do(req)
        if err == nil && resp.StatusCode < 500 {
            resp.Body.Close()
            return true
        }
    }
    return false
}

var indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Event Gateway Demo</title>
<script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 p-4">
  <div class="max-w-3xl mx-auto">
    <h1 class="text-3xl font-bold mb-4">Event Gateway Demo</h1>
    <p class="mb-4 text-gray-700">This page demonstrates an event router built with Bento. Buttons emit events via a specific input, the pipeline sets routing flags in processors, and each output applies its own transformation before delivery.</p>
<div class="mb-4 p-3 bg-slate-50 border border-slate-200 rounded">
      <strong>Routing Rules</strong>
      <ul class="list-disc ml-6 mt-2">
        <li><code>ORDER_CREATED</code> → Kafka (encoded as Protobuf <code>event.Event</code>)</li>
        <li><code>USER_REGISTERED</code> → RabbitMQ (JSON)</li>
        <li><code>AUDIT_LOG</code> → HTTP collector (JSON)</li>
        <li><code>BROADCAST_DEMO</code> → Kafka + RabbitMQ + HTTP (fan‑out)</li>
      </ul>
      <p class="mt-2 text-xs text-slate-500">Routing logic is in processors (sets meta(route_*)), transformations are in output cases (e.g., Kafka → Protobuf).</p>
    </div>
    <table class="w-full mb-4 text-sm">
      <thead><tr class="text-left">
        <th class="py-2 pr-4">Scenario (what it demonstrates)</th>
        <th class="py-2">Emit</th>
      </tr></thead>
      <tbody class="align-middle">
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">ORDER_CREATED: CE → Protobuf mapping → Kafka</td><td><button class="px-3 py-1 bg-blue-600 text-white rounded" onclick="emit('/emit/order-created')">Emit</button></td></tr>
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">USER_REGISTERED: CE → JSON → RabbitMQ</td><td><button class="px-3 py-1 bg-orange-600 text-white rounded" onclick="emit('/emit/user-registered')">Emit</button></td></tr>
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">AUDIT_LOG: CE → JSON → HTTP (collector)</td><td><button class="px-3 py-1 bg-slate-700 text-white rounded" onclick="emit('/emit/audit')">Emit</button></td></tr>
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">BROADCAST_DEMO: Fan‑out to Kafka + RabbitMQ + HTTP</td><td><button class="px-3 py-1 bg-purple-600 text-white rounded" onclick="emit('/emit/broadcast')">Emit</button></td></tr>
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">Person JSON: Non‑CE JSON → default route (HTTP)</td><td><button class="px-3 py-1 bg-green-600 text-white rounded" onclick="emit('/emit/person')">Emit</button></td></tr>
        <tr class="border-t border-gray-300"><td class="py-2 pr-4">Inbound AMQP: Send one message directly to inbound‑queue</td><td><button class="px-3 py-1 bg-emerald-600 text-white rounded" onclick="emit('/emit/inbound-amqp')">Emit</button></td></tr>
      </tbody>
    </table>
        <div id="inputs" class="p-4 mb-4 border-2 border-gray-300 rounded-md">
      <h2 class="text-xl font-semibold mb-2">Inputs</h2>
      <div id="httpInputBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">HTTP Input (POST /inbound)</h3>
        <div id="httpInputStatus" class="text-sm mt-1">Checking status...</div>
      </div>
      <div id="amqpInBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">AMQP Inbound (sender → inbound-queue)</h3>
        <div id="amqpInStatus" class="text-sm mt-1">Checking status...</div>
        <div id="amqpInMessages" class="mt-2 text-sm"></div>
      </div>
    </div>
    <div id="outputs" class="p-4 mb-4 border-2 border-gray-300 rounded-md">
      <h2 class="text-xl font-semibold mb-2">Outputs</h2>
      <div id="kafkaOutBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">Kafka (ORDER_CREATED → Protobuf)</h3>
        <div id="kafkaOutStatus" class="text-sm mt-1">Checking status...</div>
        <div id="kafkaOutMessages" class="mt-2 text-sm"></div>
      </div>
      <div id="amqpOutBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">RabbitMQ (USER_REGISTERED → JSON)</h3>
        <div id="amqpOutStatus" class="text-sm mt-1">Checking status...</div>
        <div id="amqpOutMessages" class="mt-2 text-sm"></div>
      </div>
      <div id="httpOutBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">HTTP Output (collector)</h3>
        <div id="httpOutMessages" class="mt-2 text-sm"></div>
      </div>
    </div>
  </div>
<script>
  async function emit(path){
    try{
      const res = await fetch(path,{method:'POST'});
      console.log('emit', path, res.status);
    }catch(e){console.error('emit failed',e)}
  }
  const es = new EventSource('/sse');
  const el = id => document.getElementById(id);
  const limit = (c,m=5)=>{ while(c.children.length>m){ c.removeChild(c.lastChild) } };
  es.addEventListener('outputKafkaStatus', e=>{ el('kafkaOutStatus').textContent='Status: '+e.data; });
  es.addEventListener('outputAmqpStatus', e=>{ el('amqpOutStatus').textContent='Status: '+e.data; });
  es.addEventListener('inputAmqpStatus', e=>{ el('amqpInStatus').textContent='Status: '+e.data; });
  es.addEventListener('outputKafkaMessage', e=>{ const d=document.createElement('div'); d.textContent=e.data; el('kafkaOutMessages').prepend(d); limit(el('kafkaOutMessages')); });
  es.addEventListener('outputAmqpMessage', e=>{ const d=document.createElement('div'); d.textContent=e.data; el('amqpOutMessages').prepend(d); limit(el('amqpOutMessages')); });
  es.addEventListener('inputAmqpSent', e=>{ const d=document.createElement('div'); d.textContent=e.data; el('amqpInMessages').prepend(d); limit(el('amqpInMessages')); });
  es.addEventListener('outputHttpMessage', e=>{ const d=document.createElement('div'); d.textContent=e.data; el('httpOutMessages').prepend(d); limit(el('httpOutMessages')); });
</script>
</body>
</html>`
