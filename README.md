# Central Patch Monitor 0.7.3

See [0.7.3 changes and upgrade instructions](docs/UPGRADE_0.7.3.md) for saved-flow deletion, inventory filter fixes, and stop-request concurrency fixes. The 0.7.2 reference below remains the architectural baseline; the 0.7.3 note supersedes its statements about missing flow deletion and the fixed stop behavior. Current HTTP schema: [central-openapi.json](docs/central-openapi.json).

# Central Patch Monitor: documentation entrypoint

For AI models and engineers maintaining central version **0.7.2**, start with:

1. [Model context and reading guide](docs/MODEL_CONTEXT.md)
2. [Complete 0.7.2 technical reference](docs/TECHNICAL_REFERENCE_0.7.2.md)
3. [Central HTTP OpenAPI](docs/central-openapi.json) and [configuration JSON Schema](docs/central-config.schema.json)

The reference documents actual implementation details, API lifecycle, SQLite transactions, scope/matching rules, deployment, debugging, and known limitations. The older distributed master/agent mode remains in this repository and must not be confused with the central entrypoint `app.central:create_app`.

# 0.7.2 — Çoklu namespace deseni, akış seçimi ve ayrı sürüm/sağlık karşılaştırması

Güncelleme: [UPGRADE_0.7.2.md](docs/UPGRADE_0.7.2.md). Kapanmış oturumlar için varsayılan otomatik saklama süresi 14 gündür.

# 0.7.1 — Cluster and scan debug logs

See [LOGGING.md](docs/LOGGING.md) for PATCH_LOG_LEVEL, operational event meanings and update commands.

# 0.7.0 — Guided flows and filtered live monitoring

See [UPGRADE_0.7.0.md](docs/UPGRADE_0.7.0.md) for current build/update commands, UI workflow and API additions. Existing deployments: update the image only to preserve configured remote connections.

# Central Patch Monitor 0.6.0

Yeni merkezi mod: tek pod, ConfigMap flow yönetimi, çoklu API bağlantısı, SQLite baseline ve iki canlı image tablosu. LLM veya uzak agent gerektirmez.

**Kurulum, demo ve API sözleşmesi:** [docs/CENTRAL_MONITOR.md](docs/CENTRAL_MONITOR.md).

Build: `Dockerfile.central`. Deployment: yalnızca `deploy/central/`. Demo config: `config/central.demo.yaml`.

Aşağıdaki bölüm önceki dağıtık modun belgesidir; merkezi kurulum için bu manifestleri kullanmayın.

---

# OpenShift AI Assistant 0.5.1

Her OpenShift cluster'ında çalışan, master kontrollü ve normal zamanda `IDLE` kalan
read-only patch agent ile aynı image içinden çalıştırılan web dashboard/master servisi.

Paket hiçbir harici UI framework'ü, CDN, npm modülü veya yeni Python bağımlılığı
kullanmaz. Dashboard, FastAPI `HTMLResponse` ve paket içine gömülü vanilla
HTML/CSS/JavaScript ile sunulur.

## Dashboard

Master Route'unun `/` adresi aşağıdaki ekranı sunar:

- Agent ONLINE/STALE/OFFLINE durumu ve son heartbeat yaşı
- IDLE/RUNNING durumu, aktif process ve target tag
- UI üzerinden START/STOP kontrolü
- 1-720 dakika arası izleme oturumu ve varsayılan 60 dakikalık sayaç
- Server-Sent Events ile iki saniyelik canlı veri akışı
- Açık JSON detaylarını koruma, stream görünümünü duraklatma ve collector filtresi
- Operasyon açıklamalı rollout, health ve yapılandırılmış Crash/LLM panelleri
- Bağlı agent environment, namespace, sürüm, heartbeat ve capability envanteri
- Süre bitiminde server-side otomatik durdurma
- Çalışma anlık görüntüsünü JSON rapor olarak indirme
- Image coverage, eşleşmeyen workload ve container listesi
- Pod/workload/node/operator/MCP health problemleri
- Crash pod ayrıntıları ve varsa LLM analizi
- Ham collector eventleri ve son çalışma geçmişi

Dashboard yalnızca kendi origin'indeki master API'lerine istek atar. Harici asset,
JavaScript veya font indirmez. Master'ın HTTP sözleşmesi
`docs/MASTER_CONTRACT.md` içindedir.

## Temel davranış

1. Pod açılır ve local ServiceAccount ile in-cluster Kubernetes client hazırlar.
2. Master'a register olur.
3. Normal zamanda yalnızca control poll ve heartbeat yapar; OCP resource collector çalıştırmaz.
4. Master süreli `RUNNING` komutu gönderince seçilen flow başlar ve kalan süreyi server-side takip eder.
5. Master `STOPPED/IDLE` gönderirse collector task'ları iptal edilir.
6. İzleme süresi biterse master otomatik `STOPPED` komutu üretir.
7. Master erişimi kaybolursa lease süresi sonunda agent kendini otomatik `IDLE` duruma alır.

Agent'a inbound Route gerekmez; bağlantılar agent → merkezi master yönündedir.

## ConfigMap ile süre ve flow yönetimi

Bütün süreler `deploy/01-configmap.yaml` içindeki `flows.yaml` alanındadır:

| Ayar | Varsayılan | Görev |
| --- | ---: | --- |
| `control_poll_seconds` | 10 sn | IDLE/RUNNING komutu kontrolü |
| `heartbeat_seconds` | 30 sn | Master'a durum gönderme |
| `default_lease_seconds` | 120 sn | Master yenilemezse otomatik durma |
| `flow_reload_seconds` | 30 sn | ConfigMap flow değişikliğini yeniden yükleme |
| `shared_cache_seconds` | 3 sn | Collector'lar arası Kubernetes liste cache'i |
| `image-rollout.interval_seconds` | 3 sn | Target image tag takibi |
| `health-errors.interval_seconds` | 30 sn | Pod/workload/node/operator/MCP hata taraması |
| `crash-triage.interval_seconds` | 30 sn | Crash pod describe+event+LLM triage |

ConfigMap volume güncellendiğinde agent flow dosyasının SHA-256 değerini fark eder. Aktif process aynı `process_id` ve kalan lease ile yeni interval'larda tekrar kurulur.

Environment namespace kapsamı deployment sırasında açıkça verilmelidir. Örnekler:

| Environment | Pattern |
| --- | --- |
| TEST | `^test-` |
| BETA | `^beta-` |
| PROD | `^prod-` |

Pattern yalnızca ConfigMap'ten gelir; master veya API kullanıcısı kapsamı genişletemez.

## Token ve API tüketimi optimizasyonu

Agent bütün cluster objelerini LLM'e göndermez.

- `health_errors` yalnızca problemli Pod, incomplete Deployment, NotReady/pressure Node, unavailable/degraded ClusterOperator ve degraded MCP kayıtlarını üretir.
- `crash_triage` yalnızca CrashLoopBackOff, ImagePullBackOff, ErrImagePull, container creation error ve OOMKilled podlarını seçer.
- `kubectl describe` çalıştırmak yerine Pod status/condition/owner/container state ve ilgili Warning Event'lerden kompakt bir structured describe üretir.
- Crash pod image tag'leri `1.4.1-hash → 1.4.1` biçiminde normalize edilir.
- Aynı crash fingerprint'i değişmeden devam ediyorsa LLM tekrar çağrılmaz.
- Crash düzeldiğinde LLM çağrısı yapılmadan master'a değişiklik eventi gönderilir.
- `max_crash_pods`, `max_events_per_pod`, `max_errors` ve `max_mismatches` flow içinden sınırlandırılır.
- Aynı periyotta pod verisine ihtiyaç duyan collector'lar kısa süreli ortak cache kullanır.

`emit_only_changes: true` olduğunda aynı sonuç her interval'da master'a tekrar gönderilmez. Event gönderimi başarısızsa fingerprint işaretlenmez; sonraki interval'da yeniden denenir.

## Image tag izleme

Master process başlatırken `target_tag` verir:

```json
{
  "desired_state": "RUNNING",
  "process_id": "patch-test-20260901",
  "flow_name": "monthly-ocp-patch",
  "target_tag": "1.4.1",
  "lease_seconds": 120,
  "parameters": {"monitoring_duration_seconds": 3600}
}
```

Şu tag'ler aynı kabul edilir:

```text
1.4.1
1.4.1-a81bc223
1.4.1-928472
```

Image kontrolü yalnızca `PATCH_AGENT_REGISTRY_PREFIX` ile başlayan application image'larına uygulanır. Varsayılan manifest değeri:

```text
registry.example.com/
```

Harici sidecar image'ları target coverage hesabına girmez.

## LLM

LLM adresi ve model ConfigMap'tedir:

```text
LLM_URL=https://llm.example.com/v1/chat/completions
LLM_MODEL=<model-name>
```

443 HTTPS portudur. TLS doğrulaması açıktır; kurum CA bundle'ı volume olarak mount edilir.

Token yalnızca Secret'tan gelir:

```text
LLM_TOKEN
```

LLM deterministik crash tespitini değiştirmez. Yalnızca kompakt crash kanıtından Türkçe özet, olası neden, confidence ve read-only kontrol önerileri üretir.

## Güvenlik

- Agent ServiceAccount yalnızca `get/list/watch` yetkilerine sahiptir.
- Secret, ConfigMap, create, update, patch ve delete yetkisi yoktur.
- Master trafiği cluster'a özel `AGENT_TOKEN` ile HMAC-SHA256 imzalanır.
- İmza timestamp, HTTP method, path ve body hash'ini kapsar.
- ServiceAccount, agent ve LLM tokenları response/log/event içine yazılmaz.
- Master ve LLM bağlantısında `verify=false` kullanılmaz.
- Container root olmayan kullanıcı, read-only root filesystem ve dropped capabilities ile çalışır.

## Dosyalar

Tam yapı `FILE_TREE.txt` içindedir.

- `app/runtime.py`: IDLE/RUNNING, START/STOP ve lease state machine
- `app/master.py`: master API, agent HMAC endpointleri ve gömülü dashboard UI
- `app/scheduler.py`: ConfigMap collector scheduler
- `app/collectors.py`: image, error-only health ve crash describe collector'ları
- `app/llm.py`: değişiklik bazlı crash triage
- `app/transport.py`: master protocol ve HMAC taşıma
- `deploy/`: OpenShift SA/RBAC/ConfigMap/Secret/Deployment
- `BUILD.md`: `oc new-build` ve binary build adımları
- `openapi.json`: agent health/state JSON sözleşmesi

## Test

```bash
python -m pip install -r requirements.txt
PYTHONPATH=. python -m unittest discover -s tests -v
```

Test kapsamı:

- ConfigMap interval yükleme
- Image tag normalization
- HMAC method/path/body binding
- Yalnızca crash pod describe edilmesi
- Değişmeyen crash için ikinci kez LLM çağrılmaması
- Master START/STOP komutuyla collector açılıp kapanması
- Dashboard'un dış asset kullanmaması ve gerekli API route'larının bulunması

## Base64 paketi çözme

Teams'ten gelen `.zip.b64` dosyası Linux üzerinde:

```bash
base64 -d ocp-patch-agent-v0.5.1.zip.b64 > ocp-patch-agent-v0.5.1.zip
unzip ocp-patch-agent-v0.5.1.zip
cd ocp-patch-agent
```

Windows PowerShell:

```powershell
[IO.File]::WriteAllBytes(
  "ocp-patch-agent-v0.5.1.zip",
  [Convert]::FromBase64String(
    (Get-Content "ocp-patch-agent-v0.5.1.zip.b64" -Raw)
  )
)
```

Sonraki adım `BUILD.md` üzerinden macOS cihazında Podman build alıp public Quay
repository'ye göndermektir. Kuruma özel deployment manifestleri bastion üzerinde
ayrı olarak uygulanır.
