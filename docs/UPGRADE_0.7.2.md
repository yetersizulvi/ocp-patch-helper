# 0.7.2 güncellemesi

Namespace desenleri virgülle ayrılabilir: `test-*,uat-*`. OR anlamına gelir; boşluklar temizlenir, tekrarlar kaldırılır. Akış kapsamı, toplama, tablo filtreleri ve SQL karşılaştırması aynı kurala uyar. Belirli namespace listesi ve yöneticinin cluster regex kısıtı ayrıca uygulanır.

Akış sekmesinde kayıtlı akışlar hedef/kapsam/aralık bilgisiyle seçilir. Seçmek izleme başlatmaz. Aynı adla kaydetmek günceller; yeni adla kaydetmek kopya oluşturur. Test, UAT ve ikisini birlikte seçen başlangıç düğmeleri bulunur.

Önce/sonra tablosunda iki bağımsız sonuç vardır:
- Sürüm durumu: tümü hedefte, geçmemiş, karışık, bilinmeyen veya kaldırılmış.
- Sağlık değişimi: hatalı pod sayısı arttı/azaldı, hata sürüyor, düzeldi, tümü sağlıklı, hazır değil veya yeni kaynak.

API: `GET /api/v1/sessions/{id}/changes?version_status=NOT_UPDATED&health_change=PERSISTING_ERROR`. Eski `classification` alanı uyumluluk için korunur. Summary `health_counts` ekler. Karşılaştırma workload/container başına pod sayılarını değerlendirir; eşit hata sayısı aynı podun aynı hatası demek değildir.

## Otomatik saklama

`storage.retention_days: 14`: kapanmasının üzerinden 14 gün geçen STOPPED/COMPLETED/INTERRUPTED oturumları ve bağlı baseline/current verileri otomatik silinir. Bakım ConfigMap yenileme aralığında çalışır, ayrıca yeni oturum oluştururken kontrol edilir. Açık oturumlar ve kayıtlı akışlar korunur. Eski şemaya `closed_at` otomatik eklenir; eski kapalı kayıtlar için mevcut ends, yoksa created zamanı kullanılır. SQLite boş sayfaları yeniden kullanır, dosya her temizlikte küçültülmez. Temizlik gerçekleşirse pod logunda `retention_cleanup` görülür.

Mevcut ConfigMap’te 7 yazıyorsa yeni varsayılan bunu değiştirmez. Merkezi cluster bastionunda:

```bash
oc edit configmap patch-monitor-config -n patch-monitor
```

`data.central.yaml` içindeki mevcut `storage` bölümünde yalnızca `retention_days` değerini `14` yap. Diğer bağlantı, token/CA mount ve storage path ayarlarını koru. Bu ayar yeni sürümde canlı yeniden yüklenebilir.

## Build ve mevcut deployment güncellemesi

Kaynak klasöründe Docker ile (cluster worker mimarisi amd64 ise):

```bash
docker login quay.io
docker build --platform linux/amd64 -f Dockerfile.central -t quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.2 .
docker push quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.2
```

Merkezi cluster bastionunda:

```bash
oc set image deployment/patch-monitor monitor=quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.2 -n patch-monitor
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
oc logs -f deployment/patch-monitor -n patch-monitor --since=5m
```

Mevcut kurulumun üzerine generic ConfigMap manifestini uygulama; uzak cluster bağlantısını kendi configinle koru. Yeni image build/push veya cluster rollout bu çalışma ortamında yapılmadı.

Doğrulama: Python testleri, gerçek demo HTTP/SSE smoke testi, gerçek API’ye bağlı jsdom akış seçimi ve gezinme testi. Görsel tarayıcı doğrulaması ve gerçek cluster yük testi yapılmadı.
