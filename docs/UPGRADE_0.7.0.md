# Patch Monitor 0.7.0 — Kullanım ve mevcut kurulumu güncelleme

## Ne değişti?

Ekran dört sekmeye ayrıldı: Akış tasarla, Canlı izle, Önce / sonra, Oturum geçmişi. Yeni akış tasarımı cluster seçimi, namespace glob deseni (`test-*`, `uat-*`, `test-payment-*`), isteğe bağlı virgülle ayrılmış namespace listesi, hedef sürüm, exact/release eşleştirme, süre ve tarama aralığı içerir. Sağdaki açıklama yapılan seçimleri gösterir. Önizleme sunucu doğrulaması yapar; tarama başlatmaz. Tasarım adı ve açıklamayla SQLite'a kaydedilebilir (en fazla 50 tasarım; aynı isim güncellenir).

Akış kapsamı collector tarafında uygulanır: yönetici ConfigMap'indeki namespace regex'i ile kullanıcının glob/listesi kesiştirilir. Kullanıcı bir cluster adresi/tokenı veremez ve izin verilen kapsamı genişletemez. Tarama aralığı ConfigMap şablonundan daha kısa seçilemez. Baseline alındıktan sonra canlı izleme kullanıcı tarafından başlatılır. Oturum sırasında akış kapsamı değişmez.

Canlı ekrandaki cluster / namespace / glob / uygulama-image araması filtreleri yalnızca görünümü değiştirir. SQL sorguları hem sayaçları hem tabloları filtreler. Önce/sonra araması, eşleşen workload/container'ın iki snapshot'ını birlikte karşılaştırır; yalnızca hedef image'ı aramak eski snapshot'ı kaybedip sahte NEW_RESOURCE sonucu üretmez.

Her cluster kartı oturumda seçilip seçilmediğini, son başarılı veri okuma zamanını, güncelliğini ve hata bilgisini gösterir. Kart sayaçları aynı namespace/arama/sidecar filtreleriyle hesaplanır; cluster seçme filtresi kartların kendi cluster'ını değiştirmez. Bir cluster'ın configte kayıtlı olması bağlantısının doğrulandığı anlamına gelmez. Ekran bağlantısının açık olması da Kubernetes API bağlantısının sağlıklı olduğunu tek başına göstermez.

Mesh sidecar'larını gizle seçeneği `istio-proxy` ve `linkerd-proxy` container adlarını görüntü sorgularından çıkarır. Tespit ada dayalıdır, diğer yardımcı container'ları otomatik tanımaz. Ham baseline/current kayıtları korunur, seçenek kapatılarak görülebilir. Tag'i bilinmeyen image'lar ayrı sayılır; hedef dışındaki bilinen tag'lerle karıştırılmaz. İlerleme yüzdesi tüm görüntülenen gerçek container'lardan hedef tag'e eşleşenlerin oranıdır, sağlıklı replica oranı değildir. Pod bekleyen desired satırları bu paydaya girmez.

Yeni akış formunun varsayılanı **sürüm ailesi**. ConfigMap'teki release_pattern kullanılır; standart desenle hedef `1.4.1`, `1.4.1-1.0.44` ve `1.4.1-3.0.6` gibi tag'leri eşleştirir, `1.4.10` eşleşmez. Birebir eşleşme ayrıca seçilebilir. Eski oturumların eşleştirme yöntemi/baseline'ı değiştirilmez; ekran onu açıkça gösterir.

## Güncelleme

Yeni kaynak ZIP'ini ayrı bir klasöre çıkar. Mac'te bu klasörde:

```bash
docker login quay.io --username REPLACE_WITH_QUAY_USERNAME
docker build --platform linux/amd64 -f Dockerfile.central -t quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.0 .
docker push quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.0
```

Aktif izlemeyi UI'dan durdur. Merkezi cluster bastionunda:

```bash
oc set image deployment/patch-monitor monitor=quay.io/REPLACE_WITH_QUAY_NAMESPACE/ocp-patch-agent:0.7.0 -n patch-monitor
oc rollout status deployment/patch-monitor -n patch-monitor --timeout=180s
```

**Mevcut kurulumda örnek ConfigMap/Deployment manifestlerini yeniden apply etme.** Yukarıdaki image güncellemesi mevcut cluster bağlantılarını, Secret ve CA mountlarını, Route'u ve PVC'yi korur. Kaynak kodundaki generic örnek manifestler yeni kurulum içindir. Tarayıcıyı yenile; Akış tasarla sekmesinde iki cluster'ı seç ve yeni oturum başlat. Kaydedilen akışlar SQLite'ta yeni `designs` tablosunda saklanır; eski tabloların içeriği değiştirilmez. 0.7.0 oturumları yeni scope alanı içerdiğinden 0.6.0'a geri dönüş planında veritabanı sürüm uyumluluğu ayrıca değerlendirilmelidir.

## İlk akış örneği

1. Hedef: `1.4.1`; eşleştirme: Sürüm ailesi.
2. İki bağlı cluster'ı seç.
3. Yalnızca test uygulamaları için `test-*`, yalnızca UAT için `uat-*`; ConfigMap'in izin verdiği tüm namespace'ler için `*`.
4. Şablon: patch-live; aralık: en az şablonun minimumu (örneğin 5 saniye); süre: 60 dakika.
5. Akışı önizle. İstersen ad ve açıklama verip kaydet.
6. Oturum oluştur ve başlangıcı kaydet. Bu işlemi patch başlamadan önce yap.
7. Başlangıç hazır olunca Canlı izlemeyi başlat.
8. Cluster/namespace filtreleriyle geçişi incele; Önce/sonra sekmesinde yeni bozulan ve düzelenleri gör.

## API eklemeleri

- `GET /api/v1/flows`: ConfigMap şablonları ve kaydedilmiş tasarımlar.
- `POST /api/v1/flows/preview`: NewSession gövdesini doğrular ve akış adımlarını gösterir.
- `POST /api/v1/flows/designs`: `{name, description, settings}` kaydeder; settings, oturum oluşturma gövdesidir.
- `GET /api/v1/sessions/{id}/facets?cluster=...`: Snapshot'lardaki namespace seçenekleri (en fazla 2000).
- Oturum oluşturma ek alanları: `namespace_glob`, `namespaces`, `interval_seconds`, `tag_match_mode`.
- Summary/images/targets/changes ortak filtreleri: `cluster`, `namespace`, `namespace_glob`, `search`, `hide_infrastructure`.
- Images ek filtresi `known_tag_only`; targets sağlık filtresi `ERRORS` tüm crash/image-pull/startup hatalarını kapsar.
- Summary: `tag_known` bazında sayaçlar ve `cluster_counts`. Eski alanlar korunur.

## Test

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
python scripts/smoke_central.py
```

İsteğe bağlı DOM etkileşim testi (geliştirme bağımlılığı; uygulama image'ına gerekmez):

```bash
npm install --prefix /tmp/patch-dom-tests jsdom
PATCH_JSDOM_MODULE=/tmp/patch-dom-tests/node_modules/jsdom PATCH_TEST_PYTHON=python node scripts/test_ui_dom.cjs
```

DOM testi gerçek demo API sunucusu üzerinde akış kaydetme/oluşturma, baseline/start, cluster filtresi ve sayaçlar, namespace glob, menü geçişi, geçmiş ve durdurmayı kontrol eder. Bu görsel tarayıcı testi değildir. Bu çalışma ortamında tarayıcı yerel URL'ye erişimi engelledi; görsel doğrulama tamamlanamadı. Gerçek cluster erişimi ve image build/push kullanıcı ortamında yapılmalıdır.

Doğrulama sonucu: 31 Python testi, gerçek HTTP/SSE smoke testi ve gerçek demo backend üzerinde DOM etkileşim testi geçti. DOM kapsamına duraklatılmış görünümde cluster filtresi değiştirme de dahildir.
