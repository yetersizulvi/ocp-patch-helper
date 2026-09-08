# 0.7.0 — Guided flows and filtered live monitoring

See [UPGRADE_0.7.0.md](docs/UPGRADE_0.7.0.md) for current build/update commands, UI workflow and API additions. Existing deployments: update the image only to preserve configured remote connections.

# Central mode 0.6.0

For the new one-pod monitor, build with `podman build -f Dockerfile.central -t REGISTRY/PROJECT/patch-monitor:0.6.0 .` and use only `deploy/central/`. See [CENTRAL_MONITOR.md](docs/CENTRAL_MONITOR.md) for configuration and deployment. The instructions below describe the legacy distributed entrypoints.

---

# macOS Build and Public Quay Push

Image macOS cihazında build edilir ve public Quay repository'ye gönderilir.
Kurum cluster'ı, endpoint'leri, registry prefix'i, model ve token değerleri image
içine yazılmaz. Bunlar OpenShift deployment aşamasında ConfigMap ve Secret ile
verilir.

## 1. Değişkenler

```bash
export QUAY_USER="REPLACE_WITH_QUAY_LOGIN_USER"
export QUAY_IMAGE="quay.io/REPLACE_WITH_QUAY_NAMESPACE/openshift-ai-assistant:0.5.1"
```

## 2. Quay login

```bash
podman login quay.io --username "${QUAY_USER}"
```

## 3. Lokal build

Bu komut kaynak dizininin içinde çalıştırılır:

```bash
podman build \
  --file Dockerfile \
  --tag "${QUAY_IMAGE}" \
  .
```

## 4. Image içeriği doğrulaması

```bash
podman run --rm "${QUAY_IMAGE}" \
  python -c 'from app import __version__; from app.master import DASHBOARD_HTML; assert "OpenShift AI Assistant" in DASHBOARD_HTML; print(__version__)'
```

Beklenen sürüm:

```text
0.5.1
```

## 5. Public Quay push

```bash
podman push "${QUAY_IMAGE}"
```

Repository public yapıldıktan sonra anonim erişimi doğrulayın:

```bash
podman logout quay.io
podman pull "${QUAY_IMAGE}"
```

## 6. OpenShift deployment

Deployment işlemleri bastion üzerinden yapılır. `deploy/` altındaki manifestlerde
aşağıdaki değerler kuruma göre deployment sırasında doldurulur:

```text
PATCH_AGENT_CLUSTER_ID
PATCH_AGENT_ENVIRONMENT
PATCH_AGENT_NAMESPACE_PATTERN
PATCH_AGENT_MASTER_URL
PATCH_AGENT_REGISTRY_PREFIX
LLM_URL
LLM_MODEL
AGENT_TOKEN
LLM_TOKEN
internal CA bundle
Quay image adı
```

Bu değerlerin hiçbiri public image build context'ine eklenmemelidir.
