# Master ↔ Agent HTTP Contract v1

## Dashboard ve operator API

Master `GET /` üzerinden paket içine gömülü dashboard'u sunar. Dashboard dış
CDN veya asset kullanmaz ve yalnızca aynı origin'deki aşağıdaki API'leri çağırır:

```text
GET  /api/v1/summary
GET  /api/v1/state
GET  /api/v1/events
GET  /api/v1/heartbeats
GET  /api/v1/acks
GET  /api/v1/runs
GET  /api/v1/agents
GET  /api/v1/stream
POST /api/v1/start
POST /api/v1/stop
```

`GET /api/v1/stream`, `snapshot` isimli Server-Sent Event'leri iki saniyede bir
gönderir. Payload summary, master state, son 200 collector eventi ve run
geçmişine ek olarak bağlı agent envanterini içerir. Browser bağlantı kopunca
otomatik yeniden bağlanır.

`GET /api/v1/agents` registration ve heartbeat verisinden agent id, environment,
namespace kapsamı, sürüm, flow, capability, runtime state ve bağlantı durumunu
döndürür. Token veya ServiceAccount bilgisi döndürülmez.

START body örneği:

```json
{
  "target_tag": "1.4.1",
  "duration_minutes": 60
}
```

Süre 1-720 dakika arasındadır. Süre dolunca master agent'ın sonraki control
poll isteğinde `STOPPED` komutu üretir.

`deploy/04-master.yaml` master Service, Deployment ve TLS edge Route objelerini
içerir. Kuruma özel cluster, endpoint, image ve Secret değerleri yalnızca
deployment sırasında verilir.

Agent inbound Route açmaz. Bütün bağlantıları agent başlatır.

## Authentication

Her agent'ın cluster'a özel `AGENT_TOKEN` Secret'ı vardır. İsteklerde:

```text
X-Agent-Id: cluster-a
X-Agent-Timestamp: 1788163200
X-Agent-Signature: <hex hmac-sha256>
```

Canonical signature input:

```text
timestamp + "\n" + METHOD + "\n" + PATH + "\n" + sha256(body)
```

Master önerilen clock skew limiti: 60 saniye.

## Register

```http
POST /internal/v1/agents/register
```

Agent cluster id, environment, namespace pattern, version, flow listesi ve capability listesini yollar. Master `200`, `201` veya `202` ve JSON body dönmelidir.

## Control poll

```http
GET /internal/v1/agents/{cluster_id}/control
```

Komut yoksa:

```http
204 No Content
```

Process başlatma/lease yenileme:

```json
{
  "command_id": "cmd-20260901-001",
  "desired_state": "RUNNING",
  "process_id": "patch-test-20260901",
  "flow_name": "monthly-ocp-patch",
  "target_tag": "1.4.1",
  "lease_seconds": 120,
  "issued_at": "2026-09-01T10:00:00Z",
  "parameters": {}
}
```

Master aynı RUNNING komutunu her poll'da döndürerek lease'i yenileyebilir. Komut veya master erişimi kesilirse agent lease süresi dolunca collector'ları kapatır.

Process durdurma:

```json
{
  "command_id": "cmd-20260901-002",
  "desired_state": "STOPPED",
  "process_id": "patch-test-20260901",
  "issued_at": "2026-09-01T12:00:00Z",
  "parameters": {}
}
```

## Command acknowledgement

```http
POST /internal/v1/agents/{cluster_id}/commands/{command_id}/ack
```

Agent kabul edilen state ve açıklamayı yollar.

## Heartbeat

```http
POST /internal/v1/agents/{cluster_id}/heartbeat
```

Agent `IDLE/RUNNING`, aktif process, lease bitişi ve collector durumlarını yollar. Secret veya ServiceAccount tokenı içermez.

## Collector events

```http
POST /internal/v1/agents/{cluster_id}/processes/{process_id}/events
```

Agent yalnızca ilk sonuç veya fingerprint değişikliği olduğunda event yollar. Aynı crash/image/health durumu her interval'da yeniden taşınmaz.

Master eventleri process metadata'sına bağlar ve kendi TTL politikasına göre siler.
