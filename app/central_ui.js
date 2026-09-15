"use strict";
const $ = (id) => document.getElementById(id);
const esc = (s) =>
  String(s ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const labels = {
  TARGET_REACHED: "Tümü hedef sürümde",
  NOT_UPDATED: "Hedef sürüme geçmemiş",
  PARTLY_UNKNOWN: "Bazı tag’ler bilinmiyor",
  HEALTHY: "Tümü sağlıklı",
  IMPROVING: "Hatalı pod sayısı azaldı",
  NEW_WITH_ERRORS: "Yeni kaynak, hatalı",
  READY: "Sağlıklı",
  CRASH: "Crash",
  IMAGE_PULL_ERROR: "Image çekilemiyor",
  ERROR: "Başlatma hatası",
  NOT_READY: "Hazır değil",
  WAITING_FOR_POD: "Hedef pod bekleniyor",
  TERMINATING: "Kapanıyor",
  COMPLETED: "Tamamlandı",
  COMPLETE: "Rollout tamam",
  PROGRESSING: "Rollout sürüyor",
  FAILED: "Rollout başarısız",
  UNKNOWN: "Bilinmiyor",
  REGRESSION: "Hatalı pod sayısı arttı",
  RECOVERED: "Düzeldi",
  PERSISTING_ERROR: "Hata devam ediyor",
  OLD_VERSION: "Hedef dışında",
  MIXED_VERSION: "Eski / yeni birlikte",
  IMAGE_CHANGED: "Image değişti",
  NEW_RESOURCE: "Yeni kaynak",
  REMOVED: "Kaldırıldı",
  UNCHANGED: "Değişmedi",
  UNKNOWN_VERSION: "Tag bilinmiyor",
  FRESH: "Veri güncel",
  STALE: "Veri güncel değil",
  DRAFT: "Başlangıç kaydı gerekli",
  CAPTURING: "Başlangıç kaydediliyor",
  BASELINE_READY: "Başlangıç hazır",
  RUNNING: "Canlı izleme açık",
  STOPPED: "Durduruldu",
  STOPPING: "Durduruluyor",
  INTERRUPTED: "İzleme kesildi",
};
const badge = (s) =>
  `<span class="badge ${["READY", "RECOVERED", "FRESH", "COMPLETE"].includes(s) ? "good" : ["CRASH", "ERROR", "IMAGE_PULL_ERROR", "REGRESSION", "FAILED"].includes(s) ? "bad" : "warn"}">${esc(labels[s] || s)}</span>`;
let config,
  flows,
  sid = "",
  session = null,
  tabName = "flow",
  source = null,
  paused = false,
  busy = false,
  queued = false,
  queuedForce = false,
  epoch = 0;
let cursors = { left: "", right: "", changes: "" },
  next = {},
  history = [],
  designs = [];
async function api(path, body, method = body === undefined ? "GET" : "POST") {
  const r = await fetch(
    "/api/v1" + path,
    body === undefined
      ? { method }
      : {
          method,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await r.json();
  if (!r.ok)
    throw Error(
      typeof data.detail === "string"
        ? data.detail
        : Array.isArray(data.detail)
          ? data.detail.map((x) => x.msg).join(" · ")
          : JSON.stringify(data.detail),
    );
  return data;
}
function error(e) {
  $("error").textContent = e.message || String(e);
  $("error").hidden = false;
}
function clearError() {
  $("error").hidden = true;
}
function empty(id, text) {
  $(id).innerHTML = `<tr><td class="empty" colspan="5">${esc(text)}</td></tr>`;
}
function form() {
  return {
    target_tag: $("target").value.trim(),
    flow: $("template").value,
    clusters: [...document.querySelectorAll("#flowClusters input:checked")].map(
      (x) => x.value,
    ),
    namespace_glob: $("flowPattern").value.trim() || "*",
    namespaces: $("flowNamespaces")
      .value.split(",")
      .map((x) => x.trim())
      .filter(Boolean),
    interval_seconds: Number($("interval").value),
    duration_minutes: Number($("duration").value),
    tag_match_mode: $("match").value,
  };
}
function localPreview() {
  const x = form();
  $("flowPreview").innerHTML =
    `<b>${esc(x.target_tag || "Hedef sürüm seç")}</b> için <b>${x.clusters.length} cluster</b><p>${esc(x.clusters.join(", ") || "Cluster seçilmedi")}<br>Namespace: <b>${esc(x.namespace_glob)}</b>${x.namespaces.length ? " · " + esc(x.namespaces.join(", ")) : ""}<br>Her <b>${x.interval_seconds} saniyede</b>, <b>${x.duration_minutes} dakika</b>.</p><small>${x.tag_match_mode === "release" ? "Sürüm ailesi: " + esc(x.target_tag) + "-1.0.44 gibi ekli tag’ler de eşleşir." : "Birebir eşleşme: yalnızca " + esc(x.target_tag) + ". Ekli tag’ler eşleşmez."}</small>`;
}
function templateChanged() {
  const f = flows.templates[$("template").value];
  if (!f) return;
  $("interval").min = f.interval_seconds;
  $("interval").value = f.interval_seconds;
  $("duration").max = f.session_max_minutes;
  $("duration").value = Math.min(
    Number($("duration").value),
    f.session_max_minutes,
  );
  $("intervalHint").textContent =
    `Bu şablonda en az ${f.interval_seconds} saniye. Daha seyrek tarama seçebilirsin.`;
  localPreview();
}
async function loadFlows(
  selectedName = designs[Number($("design").value)]?.name,
) {
  if ($("design").value === "" && arguments.length === 0) selectedName = null;
  flows = await api("/flows");
  designs = flows.designs;
  $("flowLibrary").innerHTML =
    designs
      .map(
        (d, i) =>
          `<div class="panel pad"><button data-design="${i}"><b>${esc(d.name)}</b><small>${esc(d.description)}<br>Hedef ${esc(d.settings.target_tag)} · ${esc(d.settings.namespace_glob)}<br>${esc((d.settings.clusters || []).join(", ") || "Tüm cluster’lar")} · ${esc(d.settings.interval_seconds || "Şablon")} sn</small>Bu akışı kullan →</button><button data-delete-design="${i}" aria-label="${esc(d.name)} akışını sil">Sil</button></div>`,
      )
      .join("") +
    ["test-*", "uat-*", "test-*,uat-*"]
      .map((p) => `<button data-preset="${p}">Yeni akış: ${p}</button>`)
      .join("");
  $("flowLibrary")
    .querySelectorAll("[data-design]")
    .forEach(
      (b) =>
        (b.onclick = () => {
          $("design").value = b.dataset.design;
          $("design").onchange();
        }),
    );
  $("flowLibrary")
    .querySelectorAll("[data-delete-design]")
    .forEach((b) => {
      b.onclick = async () => {
        const d = designs[Number(b.dataset.deleteDesign)];
        if (
          !window.confirm(
            `“${d.name}” akışı silinsin mi? Mevcut oturumlar ve baseline kayıtları korunur.`,
          )
        )
          return;
        b.disabled = true;
        try {
          clearError();
          const selected =
            $("design").value !== "" &&
            designs[Number($("design").value)]?.name === d.name;
          await api(
            "/flows/designs/" + encodeURIComponent(d.name),
            undefined,
            "DELETE",
          );
          if (selected) {
            $("design").value = "";
            $("designName").value = "";
            $("designDescription").value = "";
          }
          await loadFlows();
          $("feedback").textContent =
            `“${d.name}” akışı silindi. Oturum verileri korundu.`;
        } catch (e) {
          error(e);
          b.disabled = false;
        }
      };
    });
  $("flowLibrary")
    .querySelectorAll("[data-preset]")
    .forEach(
      (b) =>
        (b.onclick = () => {
          $("design").value = "";
          $("designName").value = "";
          $("designDescription").value = "";
          $("flowNamespaces").value = "";
          $("flowPattern").value = b.dataset.preset;
          localPreview();
        }),
    );
  $("design").innerHTML =
    '<option value="">Yeni tasarım</option>' +
    designs
      .map((d, i) => `<option value="${i}">${esc(d.name)}</option>`)
      .join("");
  const selectedIndex = designs.findIndex((d) => d.name === selectedName);
  $("design").value = selectedIndex < 0 ? "" : String(selectedIndex);
}
function switchTab(name) {
  tabName = name;
  epoch++;
  document
    .querySelectorAll("[data-tab]")
    .forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  $("flowView").hidden = name !== "flow";
  $("historyView").hidden = name !== "history";
  $("monitorArea").hidden = !["live", "compare"].includes(name);
  $("liveView").hidden = name !== "live";
  $("compareView").hidden = name !== "compare";
  $("monitorTitle").textContent =
    name === "compare" ? "Önce / sonra karşılaştırması" : "Canlı geçiş takibi";
  if (name === "history") loadHistory().catch(error);
  if (["live", "compare"].includes(name)) {
    if (!sid) {
      $("sessionLabel").textContent =
        "Önce Akış tasarla sekmesinden bir oturum oluştur.";
      $("scopeBar").hidden = true;
      $("stop").disabled = true;
    } else {
      $("scopeBar").hidden = false;
      refresh(true);
    }
  }
}
async function loadHistory() {
  history = (await api("/sessions")).items;
  $("historyList").innerHTML =
    history
      .map(
        (s) =>
          `<div class="history-item"><div><b>Hedef ${esc(s.target)}</b> · ${esc(s.flow)}<br><small>${new Date(s.created * 1000).toLocaleString("tr-TR")} · ${esc(s.id.slice(0, 8))}</small></div><div>${badge(s.status)} <button data-session="${esc(s.id)}">Oturumu aç →</button></div></div>`,
      )
      .join("") ||
    '<div class="empty">Henüz oturum yok. Akış tasarla sekmesinden başla.</div>';
}
function connect(id) {
  paused = false;
  $("pause").textContent = "Görünümü duraklat";
  if (source) source.close();
  sid = id;
  session = null;
  cursors = { left: "", right: "", changes: "" };
  epoch++;
  $("filterCluster").innerHTML =
    '<option value="">Oturumdaki tüm cluster’lar</option>';
  $("filterNamespace").innerHTML =
    '<option value="">Tüm namespace’ler</option>';
  source = new EventSource(`/api/v1/sessions/${sid}/stream`);
  source.onopen = () => {
    $("connection").textContent = "● Ekran bağlantısı açık";
  };
  source.onerror = () => {
    $("connection").textContent = "Ekran bağlantısı yeniden kuruluyor…";
  };
  source.addEventListener("revision", () => {
    if (!paused && ["live", "compare"].includes(tabName)) refresh();
  });
  switchTab("live");
}
function filters() {
  const q = new URLSearchParams({
    cluster: $("filterCluster").value,
    namespace: $("filterNamespace").value,
    namespace_glob:
      $("patternPreset").value === "custom"
        ? $("filterPattern").value || "*"
        : $("patternPreset").value,
    search: $("search").value.trim(),
    hide_infrastructure: String($("hideInfra").checked),
  });
  return q;
}
function options(id, values, placeholder) {
  const value = $(id).value;
  $(id).innerHTML =
    `<option value="">${placeholder}</option>` +
    values.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`).join("");
  if (values.includes(value)) $(id).value = value;
}
function scanErrorMessage(code) {
  const messages = {
    API_HTTP_401:
      "API kimlik doğrulaması başarısız (401). Bu cluster’ın tokenı süresi dolmuş, geçersiz veya yanlış cluster’a ait olabilir. Token Secret’ını kontrol edip güncelle.",
    API_HTTP_403:
      "API okuma yetkisi reddedildi (403). ServiceAccount RBAC yetkilerini kontrol et.",
    TLS_ERROR:
      "API sertifikası doğrulanamadı. CA dosyasını ve API hostname’ini kontrol et.",
    TOKEN_FILE_UNREADABLE:
      "Token dosyası okunamıyor. Secret mount’unu ve token_file yolunu kontrol et.",
    EMPTY_TOKEN: "Token dosyası boş. Token Secret’ını güncelle.",
    CA_FILE_UNREADABLE:
      "CA dosyası okunamıyor. ConfigMap mount’unu ve ca_file yolunu kontrol et.",
    CONNECTION_ERROR:
      "API bağlantısı kurulamadı. DNS, ağ erişimi ve API adresini kontrol et.",
    API_TIMEOUT: "API isteği zaman aşımına uğradı.",
    CANCELLED: "Tarama iptal edildi; kısmi sonuçlar yayınlanmadı.",
  };
  return messages[code] || `Tarama başarısız: ${code}`;
}
function renderSession(s, sum) {
  session = s;
  $("sessionLabel").textContent =
    `Oturum ${s.id} · Hedef ${s.target} · ${s.tag_match_mode === "release" ? "Sürüm ailesi" : "Birebir tag"} · ${s.clusters.map((c) => c.cluster).join(", ")} · Kapsam: ${s.scope.namespace_glob} · ${s.flow_settings.interval_seconds} sn`;
  options(
    "filterCluster",
    s.clusters.map((c) => c.cluster),
    "Oturumdaki tüm cluster’lar",
  );
  $("statusTitle").textContent = labels[s.status] || s.status;
  const hints = {
    DRAFT: "Başlangıç kaydı henüz tamamlanmadı.",
    CAPTURING:
      "Cluster verileri okunuyor. Tamamlanınca canlı izlemeyi başlatabilirsin.",
    BASELINE_READY:
      "Patch öncesi durum kaydedildi. Şimdi canlı izlemeyi başlat; ardından patch geçişini yap.",
    RUNNING:
      "Tarama devam ediyor. Filtreler yalnızca görüntülenen sonuçları değiştirir.",
    STOPPED:
      "Oturum durduruldu. Başarılı okumalar varsa son kayıtlar gösteriliyor; yeni izleme için Akış tasarla sekmesinden yeni oturum oluştur.",
    COMPLETED:
      "İzleme süresi doldu. Sonuçları Önce / sonra sekmesinden incele.",
    INTERRUPTED:
      "Pod yeniden başlamış olabilir. Tam baseline varsa izlemeyi yeniden başlatabilirsin.",
    STOPPING: "Devam eden okumaların durması bekleniyor.",
  };
  $("statusHelp").textContent =
    (hints[s.status] || "") +
    (s.status === "RUNNING"
      ? " Kalan: " +
        Math.max(0, Math.ceil((s.ends - Date.now() / 1000) / 60)) +
        " dakika."
      : "");
  const failed = s.clusters.filter((c) => c.error && c.error !== "CANCELLED");
  const pending = s.clusters.filter((c) => !c.baseline).map((c) => c.cluster);
  if (s.status === "DRAFT" || s.status === "INTERRUPTED") {
    $("statusHelp").textContent += pending.length
      ? ` Eksik cluster’lar: ${pending.join(", ")}.`
      : "";
    if (failed.length) {
      $("statusHelp").textContent +=
        " " +
        failed
          .map((c) => `${c.cluster}: ${scanErrorMessage(c.error)}`)
          .join(" ") +
        (pending.length
          ? " Sorunu giderdikten sonra yalnızca eksik baseline kayıtlarını yeniden deneyebilirsin."
          : " Sorunu giderdikten sonra canlı izlemeyi yeniden başlatabilirsin.");
    } else if (s.status === "DRAFT") {
      $("statusHelp").textContent += " Başlangıç kaydını alarak devam et.";
    }
  }
  if (["STOPPED", "COMPLETED"].includes(s.status) && pending.length) {
    $("statusHelp").textContent +=
      ` Baseline tamamlanmadan kapandı; ${pending.join(", ")} için karşılaştırma eksik.`;
  }
  const complete = s.clusters.every((c) => c.baseline);
  const action =
    ["BASELINE_READY", "INTERRUPTED"].includes(s.status) && complete
      ? "start"
      : ["DRAFT", "INTERRUPTED"].includes(s.status) && !complete
        ? "baseline"
        : null;
  $("primaryAction").hidden = !action;
  $("primaryAction").dataset.action = action || "";
  $("primaryAction").textContent =
    action === "start"
      ? "Canlı izlemeyi başlat"
      : "Başlangıç kaydını al / tekrar dene";
  $("stop").disabled = ["STOPPED", "COMPLETED", "STOPPING"].includes(s.status);
  $("targetHeading").textContent = s.target + " hedef sürümüne geçenler";
  $("clusterCards").innerHTML = config.clusters
    .map((c) => {
      const state = sum.clusters.find((x) => x.cluster === c.id);
      const counts = (sum.cluster_counts || {})[c.id] || [];
      const ready = counts
        .filter((x) => x.target_match && x.health === "READY")
        .reduce((n, x) => n + x.count, 0);
      const bad = counts
        .filter(
          (x) =>
            x.target_match &&
            ["CRASH", "ERROR", "IMAGE_PULL_ERROR"].includes(x.health),
        )
        .reduce((n, x) => n + x.count, 0);
      const old = counts
        .filter((x) => !x.target_match && x.tag_known && x.row_type === "pod")
        .reduce((n, x) => n + x.count, 0);
      return `<div class="cluster-card"><h3>${esc(c.id)} ${state ? badge(state.freshness) : '<span class="badge">Bu oturumda seçili değil</span>'}</h3><small>${c.connection === "inCluster" ? "Bulunduğu cluster · iç API" : "Uzak cluster · API bağlantısı"}</small><p style="margin-bottom:0;font-size:13px">${state ? (state.error ? "Son tarama: " + esc(scanErrorMessage(state.error)) : state.observed ? "Son başarılı okuma: " + new Date(state.observed * 1000).toLocaleTimeString("tr-TR") : "Henüz başarılı veri okuması yok.") : "Konfigüre edilmiş. İzlemek için yeni akışta seç."}</p>${state ? `<p style="font-size:13px"><b>${ready}</b> hedefte sağlıklı · <b>${bad}</b> sorunlu · <b>${old}</b> hedef dışında<br><small>Aynı namespace ve arama filtreleri uygulanır.</small></p>` : ""}</div>`;
    })
    .join("");
}
function renderTotals(sum) {
  let t = { total: 0, ready: 0, bad: 0, old: 0, unknown: 0, pending: 0 },
    matched = 0;
  for (const r of sum.counts) {
    if (r.row_type === "pod") {
      t.total += r.count;
      if (r.target_match) matched += r.count;
      if (!r.tag_known) t.unknown += r.count;
      else if (!r.target_match) t.old += r.count;
    }
    if (r.target_match) {
      if (r.health === "READY") t.ready += r.count;
      else if (["CRASH", "ERROR", "IMAGE_PULL_ERROR"].includes(r.health))
        t.bad += r.count;
      else t.pending += r.count;
    }
  }
  Object.entries(t).forEach(([k, v]) => ($(k).textContent = v));
  const percent = t.total ? Math.round((100 * matched) / t.total) : 0;
  $("progressText").textContent =
    `${matched} / ${t.total} container hedef tag’e eşleşiyor · %${percent}`;
  $("progressFill").style.width = percent + "%";
  const cc = sum.health_counts || sum.change_counts;
  $("changeSummary").textContent =
    `${cc.REGRESSION || 0} hata sayısı artan · ${cc.RECOVERED || 0} düzelen · ${cc.PERSISTING_ERROR || 0} sorunu devam eden workload/container`;
}
function renderRows(side, data) {
  next[side] = data.next_cursor;
  $(side + "Next").disabled = !data.next_cursor;
  $(side + "Info").textContent =
    `${data.items.length} satır · sayfa en fazla 50`;
  $(side).innerHTML = data.items
    .map(
      (r) =>
        `<tr><td><b>${esc(r.workload)}</b><small>${esc(r.cluster)}<br>${esc(r.namespace)}<br>${esc(r.container)}</small></td><td><span class="tag">${esc(r.image_tag || "Tag bilinmiyor")}</span><br><small title="${esc(r.image)}">${esc(r.image)}</small></td><td>${badge(r.health)}${side === "right" ? "<br>" + badge(r.rollout) : ""}<br><small>${esc(r.reason)}</small><br><button class="detail-button" data-detail="${esc(r.id)}">Önce / şimdi ayrıntısı ↗</button></td></tr>`,
    )
    .join("");
  if (!data.items.length)
    empty(
      side,
      side === "right"
        ? "Bu filtrede hedef sürüm kaydı yok. Sağlık filtresini ve tag eşleştirme yöntemini kontrol et."
        : "Bu filtrede container yok. Cluster / namespace seçimini kontrol et.",
    );
}
function renderChanges(data) {
  next.changes = data.next_cursor;
  $("changesNext").disabled = !data.next_cursor;
  $("changes").innerHTML = data.items
    .map(
      (r) =>
        `<tr><td><b>${esc(r.workload)}</b><small>${esc(r.cluster)} · ${esc(r.namespace)}<br>${esc(r.container)}</small></td><td>${esc(r.before_images || "Kaynak yok")}<br><small>Sağlıklı: ${r.before_ready ?? 0} · Hata: ${r.before_errors ?? 0}</small></td><td>${esc(r.after_images || "Kaynak yok")}<br><small>Sağlıklı: ${r.after_ready ?? 0} · Hata: ${r.after_errors ?? 0}</small></td><td>${badge(r.version_status)}<small>${r.target_replicas ?? 0} / ${r.after_total ?? 0} pod hedefte</small></td><td>${badge(r.health_change)}<small>Hatalı pod: ${r.before_errors ?? 0} → ${r.after_errors ?? 0}</small></td></tr>`,
    )
    .join("");
  if (!data.items.length)
    empty("changes", "Bu kapsamda karşılaştırma sonucu yok.");
}
async function refresh(force = false) {
  if (!sid || (paused && !force)) return;
  if (busy) {
    queued = true;
    queuedForce = queuedForce || force;
    return;
  }
  busy = true;
  const requestEpoch = epoch,
    requestSid = sid;
  try {
    const base = "/sessions/" + sid,
      q = filters();
    const [s, sum, facets] = await Promise.all([
      api(base),
      api(base + "/summary?" + q),
      api(
        base +
          "/facets?cluster=" +
          encodeURIComponent($("filterCluster").value),
      ),
    ]);
    if (requestEpoch !== epoch || requestSid !== sid) {
      queued = true;
      return;
    }
    if (s.revision !== sum.revision || s.status !== sum.status) {
      queued = true;
      return;
    }
    renderSession(s, sum);
    renderTotals(sum);
    options("filterNamespace", facets.namespaces, "Tüm namespace’ler");
    if (tabName === "compare") {
      const c = await api(
        base +
          "/changes?" +
          q +
          "&version_status=" +
          encodeURIComponent($("versionFilter").value) +
          "&health_change=" +
          encodeURIComponent($("changeFilter").value) +
          "&cursor=" +
          (cursors.changes || 0),
      );
      if (requestEpoch === epoch) renderChanges(c);
    } else if (tabName === "live") {
      const [l, r] = await Promise.all([
        api(
          base +
            "/images?" +
            q +
            "&cursor=" +
            cursors.left +
            ($("leftFilter").value
              ? "&target_match=false&known_tag_only=true"
              : ""),
        ),
        api(
          base +
            "/targets?" +
            q +
            "&cursor=" +
            cursors.right +
            "&health=" +
            encodeURIComponent($("rightFilter").value),
        ),
      ]);
      if (requestEpoch === epoch) {
        renderRows("left", l);
        renderRows("right", r);
      }
    }
  } catch (e) {
    error(e);
  } finally {
    busy = false;
    if (queued) {
      queued = false;
      const forceNext = queuedForce;
      queuedForce = false;
      refresh(forceNext);
    }
  }
}
function changed() {
  epoch++;
  cursors = { left: "", right: "", changes: "" };
  clearError();
  refresh(true);
}
document
  .querySelectorAll("[data-tab]")
  .forEach((b) => (b.onclick = () => switchTab(b.dataset.tab)));
$("template").onchange = templateChanged;
[
  "target",
  "match",
  "flowPattern",
  "flowNamespaces",
  "interval",
  "duration",
].forEach((id) => $(id).addEventListener("input", localPreview));
$("flowClusters").onchange = localPreview;
$("validate").onclick = async () => {
  try {
    clearError();
    const p = await api("/flows/preview", form());
    $("feedback").textContent =
      `Akış geçerli: ${p.clusters.join(", ")} · ${p.scope.namespace_glob} · ${p.interval_seconds} sn.\n${p.note}`;
  } catch (e) {
    error(e);
  }
};
$("saveDesign").onclick = async () => {
  $("saveDesign").disabled = true;
  try {
    clearError();
    await api("/flows/designs", {
      name: $("designName").value.trim(),
      description: $("designDescription").value,
      settings: form(),
    });
    await loadFlows($("designName").value.trim());
    $("feedback").textContent =
      "Tasarım kaydedildi. Kayıtlı tasarım listesinden tekrar seçebilirsin.";
  } catch (e) {
    error(e);
  } finally {
    $("saveDesign").disabled = false;
  }
};
$("design").onchange = () => {
  if ($("design").value === "") {
    $("designName").value = "";
    $("designDescription").value = "";
    return;
  }
  const d = designs[Number($("design").value)],
    x = d.settings;
  if (!flows.templates[x.flow]) {
    error(
      new Error(
        "Bu akışın şablonu kaldırılmış. Geçerli bir şablon seçip yeniden kaydet.",
      ),
    );
    return;
  }
  $("template").value = x.flow;
  templateChanged();
  $("target").value = x.target_tag;
  $("match").value = x.tag_match_mode || config.images.tag_match_mode;
  $("flowPattern").value = x.namespace_glob;
  $("flowNamespaces").value = x.namespaces.join(", ");
  $("duration").value = x.duration_minutes;
  if (x.interval_seconds) $("interval").value = x.interval_seconds;
  $("designName").value = d.name;
  $("designDescription").value = d.description;
  document
    .querySelectorAll("#flowClusters input")
    .forEach((c) => (c.checked = !x.clusters || x.clusters.includes(c.value)));
  localPreview();
};
$("create").onclick = async () => {
  try {
    clearError();
    $("create").disabled = true;
    const body = form();
    await api("/flows/preview", body);
    const s = await api("/sessions", body);
    connect(s.id);
    await api("/sessions/" + s.id + "/baseline", {});
    await refresh(true);
  } catch (e) {
    error(e);
  } finally {
    $("create").disabled = false;
  }
};
$("primaryAction").onclick = async () => {
  try {
    clearError();
    await api("/sessions/" + sid + "/" + $("primaryAction").dataset.action, {});
    await refresh(true);
  } catch (e) {
    error(e);
  }
};
$("stop").onclick = async () => {
  try {
    clearError();
    $("stop").disabled = true;
    await api("/sessions/" + sid + "/stop", {});
    await refresh(true);
  } catch (e) {
    error(e);
  }
};
$("pause").onclick = () => {
  paused = !paused;
  $("pause").textContent = paused
    ? "Canlı görünümü sürdür"
    : "Görünümü duraklat";
  if (!paused) refresh();
};
[
  "filterNamespace",
  "hideInfra",
  "leftFilter",
  "rightFilter",
  "changeFilter",
  "versionFilter",
].forEach((id) => ($(id).onchange = changed));
$("filterCluster").onchange = () => {
  $("filterNamespace").value = "";
  changed();
};
$("patternPreset").onchange = () => {
  $("filterPattern").hidden = $("patternPreset").value !== "custom";
  changed();
};
let timer;
["search", "filterPattern"].forEach(
  (id) =>
    ($(id).oninput = () => {
      clearTimeout(timer);
      timer = setTimeout(changed, 300);
    }),
);
$("clearFilters").onclick = () => {
  $("filterCluster").value = "";
  $("filterNamespace").value = "";
  $("patternPreset").value = "*";
  $("filterPattern").hidden = true;
  $("search").value = "";
  $("hideInfra").checked = true;
  $("leftFilter").value = "";
  $("rightFilter").value = "";
  $("changeFilter").value = "";
  $("versionFilter").value = "";
  changed();
};
document.querySelectorAll("[data-first]").forEach(
  (b) =>
    (b.onclick = () => {
      cursors[b.dataset.first] = "";
      epoch++;
      refresh();
    }),
);
document.querySelectorAll("[data-next]").forEach(
  (b) =>
    (b.onclick = () => {
      cursors[b.dataset.next] = next[b.dataset.next] || "";
      epoch++;
      refresh();
    }),
);
document.querySelectorAll("[data-quick]").forEach(
  (b) =>
    (b.onclick = () => {
      const k = b.dataset.quick;
      $("leftFilter").value = k === "old" ? "false" : "";
      $("rightFilter").value =
        k === "READY" ? "READY" : k === "errors" ? "ERRORS" : "";
      changed();
    }),
);
$("reloadHistory").onclick = () => loadHistory().catch(error);
$("closeDetail").onclick = () => $("detail").close();
document.addEventListener("click", async (e) => {
  const h = e.target.closest("[data-session]");
  if (h) connect(h.dataset.session);
  const d = e.target.closest("[data-detail]");
  if (d)
    try {
      const data = await api(`/sessions/${sid}/containers/${d.dataset.detail}`),
        c = data.current;
      $("detailSummary").innerHTML =
        `<h3>${esc(c.workload)} · ${esc(c.container)}</h3><p>${esc(c.cluster)} / ${esc(c.namespace)}<br>${esc(c.pod || "Hedef pod henüz yok")}</p><div class="split-detail"><div class="panel pad"><b>Başlangıç</b><p>${data.baseline.map((b) => `${esc(b.image)}<br>${badge(b.health)}`).join("<hr>") || "Başlangıçta bu kaynak yok"}</p></div><div class="panel pad"><b>Şimdi</b><p>${esc(c.image)}<br>${badge(c.health)}</p><small>${esc(c.reason)}<br>Restart: ${c.restart_count}</small></div></div>`;
      $("detailText").textContent = JSON.stringify(data, null, 2);
      $("detail").showModal();
    } catch (err) {
      error(err);
    }
});
async function boot() {
  config = await api("/config");
  await loadFlows();
  $("demo").textContent = config.demo ? "DEMO · TEMSİLİ VERİ" : "";
  $("template").innerHTML = Object.keys(flows.templates)
    .map((n) => `<option value="${esc(n)}">${esc(n)}</option>`)
    .join("");
  $("flowClusters").innerHTML = config.clusters
    .map(
      (c) =>
        `<label><input type="checkbox" value="${esc(c.id)}" checked>${esc(c.id)}</label>`,
    )
    .join("");
  templateChanged();
  await loadHistory();
  $("connection").textContent = "Hazır · henüz canlı ekran bağlantısı yok";
  const active = history.find((s) =>
    ["RUNNING", "CAPTURING", "BASELINE_READY", "DRAFT", "INTERRUPTED"].includes(
      s.status,
    ),
  );
  if (active) connect(active.id);
  else {
    empty("left", "Önce bir akış oluştur.");
    empty("right", "Hedef sürüm sonuçları burada görünecek.");
  }
}
boot().catch(error);
