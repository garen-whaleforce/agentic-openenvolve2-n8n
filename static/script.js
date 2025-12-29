let selectedSymbol = null;
let selectedCompany = "";
let availableDates = [];

const searchInput = document.getElementById("search-input");
const searchBtn = document.getElementById("search-btn");
const resultsEl = document.getElementById("results");
const statusEl = document.getElementById("status");
const selectedSymbolEl = document.getElementById("selected-symbol");
const datesSelect = document.getElementById("dates-select");
const datesHint = document.getElementById("dates-hint");
const analyzeBtn = document.getElementById("analyze-btn");
const metaSymbol = document.getElementById("meta-symbol");
const metaFiscal = document.getElementById("meta-fiscal");
const metaCalendar = document.getElementById("meta-calendar");
const metaDate = document.getElementById("meta-date");
const metaCompany = document.getElementById("meta-company");
const metaSector = document.getElementById("meta-sector");
const agenticContent = document.getElementById("agentic-content");
const debugJson = document.getElementById("debug-json");
const mainModelSelect = document.getElementById("main-model-select");
const helperModelSelect = document.getElementById("helper-model-select");
const kpiPred = document.getElementById("kpi-pred");
const kpiConf = document.getElementById("kpi-conf");
const kpiReturn = document.getElementById("kpi-return");
const kpiCost = document.getElementById("kpi-cost");
const detailCmp = document.getElementById("detail-cmp");
const detailHist = document.getElementById("detail-hist");
const detailPerf = document.getElementById("detail-perf");
const detailBaseline = document.getElementById("detail-baseline");
const detailTokens = document.getElementById("detail-tokens");
const detailCmpSummary = document.getElementById("detail-cmp-summary");
const detailHistSummary = document.getElementById("detail-hist-summary");
const detailPerfSummary = document.getElementById("detail-perf-summary");
const detailBaselineSummary = document.getElementById("detail-baseline-summary");
const detailTokensSummary = document.getElementById("detail-tokens-summary");
const todayEarningsEl = document.getElementById("today-earnings");
const todayEarningsLabelEl = document.getElementById("today-earnings-label");
const todayEarningsDateEl = document.getElementById("today-earnings-date");
const todayEarningsStartInput = document.getElementById("today-earnings-start");
const todayEarningsEndInput = document.getElementById("today-earnings-end");
const todayEarningsQueryBtn = document.getElementById("today-earnings-query");
const batchInput = document.getElementById("batch-input");
const batchBtn = document.getElementById("batch-btn");
const batchStatus = document.getElementById("batch-status");
const batchResults = document.getElementById("batch-results");
const analysisProgress = document.getElementById("analysis-progress");
const cancelBtn = document.getElementById("cancel-btn");
let progressTimer = null;
let progressIndex = 0;
let progressDotTimer = null;
let abortController = null;
let lastAnalysisResult = null;
let batchPollTimer = null;
let activeBatchId = null;
const progressSteps = [
  "開始分析",
  "擷取逐字稿與財報",
  "路由與幫手分析",
  "彙整主結論",
];
let currentProgressText = "";

function setStatus(message, tone = "muted") {
  statusEl.textContent = message;
  statusEl.style.color = tone === "error" ? "#f87171" : "#9ca3af";
}

function renderResultsSkeleton() {
  const placeholder = Array.from({ length: 3 })
    .map(
      () => `<div class="skeleton-card">
        <div class="skeleton-line wide"></div>
        <div class="skeleton-line mid"></div>
      </div>`
    )
    .join("");
  resultsEl.innerHTML = placeholder;
}

function startAnalysisProgress() {
  if (!analysisProgress) return;
  progressIndex = 0;
  currentProgressText = progressSteps[progressIndex] || "開始分析";
  clearInterval(progressTimer);
  progressTimer = setInterval(() => {
    if (progressIndex < progressSteps.length - 1) {
      progressIndex += 1;
    }
    currentProgressText = progressSteps[progressIndex] || "";
  }, 5000);
  clearInterval(progressDotTimer);
  let dots = 0;
  progressDotTimer = setInterval(() => {
    if (!analysisProgress) return;
    dots = (dots + 1) % 4;
    const suffix = ".".repeat(dots || 3);
    analysisProgress.textContent = `${currentProgressText}${suffix}`;
  }, 600);
}

function stopAnalysisProgress(message = "") {
  clearInterval(progressTimer);
  progressTimer = null;
  clearInterval(progressDotTimer);
  progressDotTimer = null;
  if (analysisProgress) {
    analysisProgress.textContent = message;
  }
}

function getDefaultEarningsRange() {
  const now = new Date();
  const end = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 7);
  const fmt = (d) => d.toISOString().slice(0, 10);
  return { start: fmt(start), end: fmt(end) };
}

async function loadEarningsRange(force = false) {
  if (!todayEarningsEl) return;

  let start = todayEarningsStartInput ? todayEarningsStartInput.value : "";
  let end = todayEarningsEndInput ? todayEarningsEndInput.value : "";

  if (!start || !end) {
    const def = getDefaultEarningsRange();
    if (!start) start = def.start;
    if (!end) end = def.end;
    if (todayEarningsStartInput && !todayEarningsStartInput.value) {
      todayEarningsStartInput.value = start;
    }
    if (todayEarningsEndInput && !todayEarningsEndInput.value) {
      todayEarningsEndInput.value = end;
    }
  }

  const rangeLabel = `${start} ~ ${end}`;

  if (todayEarningsLabelEl) {
    const def = getDefaultEarningsRange();
    const isDefault = def.start === start && def.end === end;
    todayEarningsLabelEl.textContent = isDefault ? "近 7 日" : "自定區間";
  }
  if (todayEarningsDateEl) {
    todayEarningsDateEl.textContent = rangeLabel;
  }

  todayEarningsEl.innerHTML = `<p class="muted small"><span class="spinner"></span> 載入 ${rangeLabel} 財報中...</p>`;

  try {
    const url =
      `/api/earnings-calendar/range?min_market_cap=10000000000` +
      `&start_date=${encodeURIComponent(start)}` +
      `&end_date=${encodeURIComponent(end)}` +
      (force ? "&refresh=true" : "");
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();

    if (!data || !data.length) {
      todayEarningsEl.innerHTML = `<p class="muted small">${rangeLabel} 期間沒有市值超過 10B 的財報發佈</p>`;
      return;
    }

    const fmtNumber = (val) => {
      if (val === null || val === undefined) return "N/A";
      const num = Number(val);
      if (Number.isNaN(num)) return "N/A";
      return num.toFixed(2);
    };

    const rows = data
      .map(
        (item) => `<tr>
             <td>${item.date || "-"}</td>
             <td>${item.symbol || "-"}</td>
             <td>${item.company || "-"}</td>
             <td>${item.sector || "-"}</td>
             <td>${fmtNumber(item.eps_estimated)}</td>
             <td>${fmtNumber(item.eps_actual)}</td>
           </tr>`
      )
      .join("");

    todayEarningsEl.innerHTML = `
         <div class="table-wrapper">
           <table class="compact">
             <thead>
               <tr>
                 <th>Date</th>
                 <th>Symbol</th>
                 <th>Company</th>
                 <th>Sector</th>
                 <th>Est EPS</th>
                 <th>Act EPS</th>
               </tr>
             </thead>
             <tbody>
               ${rows}
             </tbody>
           </table>
         </div>
       `;
  } catch (err) {
    console.error(err);
    todayEarningsEl.innerHTML = `<p class="muted small" style="color:#f87171;">載入財報區間資料失敗：${err.message || err}</p>`;
  }
}

async function fetchDatesForSymbol(symbol) {
  try {
    const res = await fetch(`/api/transcript-dates?symbol=${encodeURIComponent(symbol)}`);
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();
    return (data || []).filter((d) => Number.isInteger(d.year) && Number.isInteger(d.quarter));
  } catch (err) {
    console.error("load dates failed for", symbol, err);
    return [];
  }
}

function renderResults(items) {
  resultsEl.innerHTML = "";
  if (!items || items.length === 0) {
    resultsEl.innerHTML = '<p class="muted small">找不到符合的公司，試試其他關鍵字。</p>';
    return;
  }

  items.forEach((item) => {
    const container = document.createElement("div");
    container.className = "result-item";

    const info = document.createElement("div");
    info.className = "info";
    const title = document.createElement("div");
    title.className = "title";
    title.textContent = `${item.symbol} - ${item.name || "Unnamed"}`;
    const meta = document.createElement("div");
    meta.className = "muted small";
    const avail = item.dates ? ` · ${item.dates.length} 筆季度` : "";
    meta.textContent = `${item.exchange || "N/A"} - ${item.currency || ""}${avail}`;

    info.appendChild(title);
    info.appendChild(meta);

    const selectBtn = document.createElement("button");
    selectBtn.textContent = "選取";
    selectBtn.onclick = () => onSelectSymbol(item);

    container.appendChild(info);
    container.appendChild(selectBtn);
    resultsEl.appendChild(container);
  });
}

async function searchSymbols() {
  const query = searchInput.value.trim();
  if (!query) {
    setStatus("請輸入要搜尋的公司名稱或代號");
    return;
  }
  renderResultsSkeleton();
  setStatus("搜尋中...");

  try {
    const res = await fetch(`/api/symbols?q=${encodeURIComponent(query)}`);
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();

    // 只保留有季度資料的公司
    const enriched = await Promise.all(
      data.map(async (item) => {
        const dates = await fetchDatesForSymbol(item.symbol);
        return { ...item, dates };
      })
    );
    const filtered = enriched.filter((x) => x.dates && x.dates.length > 0);
    renderResults(filtered);
    setStatus(`找到 ${filtered.length} 筆有季度資料的結果`);
  } catch (err) {
    console.error(err);
    setStatus(`搜尋失敗：${err.message}`, "error");
  }
}

function onSelectSymbol(item) {
  selectedSymbol = item.symbol;
  selectedCompany = item.name || item.symbol;
  selectedSymbolEl.textContent = `已選擇：${item.symbol} — ${selectedCompany}`;
  metaSymbol.textContent = item.symbol;
  metaFiscal.textContent = "-";
  metaCalendar.textContent = "-";
  metaDate.textContent = "-";
  agenticContent.innerHTML = '<p class="muted">尚未執行分析。</p>';
  debugJson.textContent = "{}";
  lastAnalysisResult = null;
  // 直接用已載入的季度資料，若沒有再補拉
  if (item.dates && item.dates.length) {
    availableDates = item.dates;
    populateDatesSelect();
  } else {
    fetchTranscriptDates(item.symbol);
  }
  // 收起其他結果
  resultsEl.innerHTML = "";
}

async function fetchTranscriptDates(symbol) {
  datesSelect.innerHTML = '<option value="">載入中...</option>';
  analyzeBtn.disabled = true;
  datesHint.textContent = "";
  try {
    availableDates = await fetchDatesForSymbol(symbol);

    if (availableDates.length === 0) {
      datesSelect.innerHTML = '<option value="">沒有可用的季度</option>';
      datesHint.textContent = "此公司尚無可用的逐字稿清單。";
      return;
    }

    populateDatesSelect();
  } catch (err) {
    console.error(err);
    datesHint.textContent = "";
    datesSelect.innerHTML = '<option value="">載入失敗</option>';
    setStatus(`載入季度失敗：${err.message}`, "error");
  }
}

function populateDatesSelect() {
  datesSelect.innerHTML = "";
  availableDates.forEach((d) => {
    const opt = document.createElement("option");
    opt.value = `${d.year}|${d.quarter}`;
    const labelDate = d.date ? d.date : "";
    const cal =
      d.calendar_year && d.calendar_quarter
        ? `CY${d.calendar_year} Q${d.calendar_quarter}`
        : `CY?`;
    opt.textContent = `${cal}${labelDate ? " / " + labelDate : ""}`;
    datesSelect.appendChild(opt);
  });
  datesHint.textContent = `共 ${availableDates.length} 筆季度資料。`;
  analyzeBtn.disabled = false;
}

function renderAgentic(result) {
  if (!result) {
    agenticContent.innerHTML = '<p class="muted">尚未執行分析。</p>';
    return;
  }
  const { prediction, confidence, summary, reasons, next_steps, metadata } = result;
  const modelMeta = metadata?.models;
  const formatModel = (model, temp) => {
    if (!model) return "-";
    if (temp == null || Number.isNaN(Number(temp))) return model;
    return `${model} (T=${Number(temp).toFixed(2)})`;
  };
  const engineLabel = modelMeta
    ? `Main: ${formatModel(modelMeta.main, modelMeta.main_temperature)} · Helpers: ${formatModel(modelMeta.helpers, modelMeta.helper_temperature)}`
    : "";
  const tokenUsage = (result.raw && result.raw.token_usage) || metadata?.token_usage || {};
  const formatReasonBody = (text) => {
    if (!text) return "<p>-</p>";
    const parts = String(text)
      .split(/\n{2,}/)
      .map((p) => p.trim())
      .filter(Boolean);
    return parts
      .map((p) => `<p>${p.replace(/\n/g, "<br>")}</p>`)
      .join("") || `<p>${text}</p>`;
  };
  const reasonsMarkup =
    reasons && reasons.length
      ? `<h4>理由</h4><div class="control-row"><button class="btn-ghost" id="reasons-expand-all">展開全部</button><button class="btn-ghost" id="reasons-collapse-all">收合全部</button><button class="btn-ghost" id="reasons-copy">複製理由</button></div><div class="accordion">${reasons
          .map((r, idx) => {
            const summary = typeof r === "string" ? r.split(" ").slice(0, 8).join(" ") + (r.split(" ").length > 8 ? "..." : "") : "理由";
            return `
            <div class="accordion-item">
              <button class="accordion-header" type="button" aria-expanded="false" data-target="reason-${idx}">
                <span>${summary}</span>
                <span class="chevron">▼</span>
              </button>
              <div id="reason-${idx}" class="accordion-body" hidden>
                ${formatReasonBody(r)}
              </div>
            </div>`;
          })
          .join("")}</div>`
      : "";
  const nextStepsMarkup =
    next_steps && next_steps.length
      ? `<h4>後續動作</h4><ul>${next_steps.map((r) => `<li>${r}</li>`).join("")}</ul>`
      : "";
  const confidenceBadge = confidence != null ? `<span class="badge">信心度 ${(confidence * 100).toFixed(0)}%</span>` : "";
  const summaryMarkup = summary ? formatReasonBody(summary) : "<p></p>";

  agenticContent.innerHTML = `
    <div class="muted small">${engineLabel}</div>
    <h4>${prediction || "N/A"} ${confidenceBadge}</h4>
    ${summaryMarkup}
    ${reasonsMarkup}
    ${nextStepsMarkup}
  `;

  // Wire up accordion toggles for reasons
  const reasonHeaders = agenticContent.querySelectorAll(".accordion-header");
  reasonHeaders.forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-target");
      const body = agenticContent.querySelector(`#${targetId}`);
      const expanded = btn.getAttribute("aria-expanded") === "true";
      btn.setAttribute("aria-expanded", String(!expanded));
      if (body) {
        if (expanded) {
          body.hidden = true;
          btn.querySelector(".chevron").style.transform = "rotate(-90deg)";
        } else {
          body.hidden = false;
          btn.querySelector(".chevron").style.transform = "rotate(0deg)";
        }
      }
    });
  });

  const expandAll = agenticContent.querySelector("#reasons-expand-all");
  const collapseAll = agenticContent.querySelector("#reasons-collapse-all");
  const copyBtn = agenticContent.querySelector("#reasons-copy");
  if (expandAll) {
    expandAll.addEventListener("click", () => {
      reasonHeaders.forEach((btn) => {
        const targetId = btn.getAttribute("data-target");
        const body = agenticContent.querySelector(`#${targetId}`);
        btn.setAttribute("aria-expanded", "true");
        if (body) {
          body.hidden = false;
          btn.querySelector(".chevron").style.transform = "rotate(0deg)";
        }
      });
    });
  }
  if (collapseAll) {
    collapseAll.addEventListener("click", () => {
      reasonHeaders.forEach((btn) => {
        const targetId = btn.getAttribute("data-target");
        const body = agenticContent.querySelector(`#${targetId}`);
        btn.setAttribute("aria-expanded", "false");
        if (body) {
          body.hidden = true;
          btn.querySelector(".chevron").style.transform = "rotate(-90deg)";
        }
      });
    });
  }
  if (copyBtn) {
    copyBtn.addEventListener("click", async () => {
      const bodies = agenticContent.querySelectorAll(".accordion-body");
      const text = Array.from(bodies)
        .map((b) => b.textContent.trim())
        .filter(Boolean)
        .join("\n\n");
      try {
        await navigator.clipboard.writeText(text || "");
        setStatus("理由已複製");
      } catch (e) {
        setStatus("無法複製理由", "error");
      }
    });
  }
}

async function runAnalysis() {
  if (!selectedSymbol) {
    setStatus("請先選擇公司", "error");
    return;
  }
  if (!datesSelect.value) {
    setStatus("請先選擇年度與季度", "error");
    return;
  }
  agenticContent.innerHTML = `<div class="control-row"><span class="spinner"></span><span class="muted small">分析中...</span></div>`;
  debugJson.textContent = "{}";
  startAnalysisProgress();
  abortController = new AbortController();
  if (cancelBtn) {
    cancelBtn.disabled = false;
  }
  lastAnalysisResult = null;
  const [yearStr, quarterStr] = datesSelect.value.split("|");
  const mainModel = (mainModelSelect && mainModelSelect.value) || "gpt-5.1";
  const helperModel = (helperModelSelect && helperModelSelect.value) || "gpt-5-mini";
  const refreshCheck = document.getElementById("refresh-check");
  const refresh = refreshCheck ? refreshCheck.checked : false;
  const payload = {
    symbol: selectedSymbol,
    year: Number(yearStr),
    quarter: Number(quarterStr),
    main_model: mainModel,
    helper_model: helperModel,
    refresh: refresh,
  };

  analyzeBtn.disabled = true;
  const originalLabel = analyzeBtn.textContent;
  analyzeBtn.textContent = "分析中...";
  setStatus("正在呼叫後端分析...");

  try {
    const res = await fetch("/api/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: abortController.signal,
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();
    metaSymbol.textContent = data.symbol || selectedSymbol;
    metaFiscal.textContent = `FY${data.year} Q${data.quarter}`;
    if (data.calendar_year && data.calendar_quarter) {
      metaCalendar.textContent = `CY${data.calendar_year} Q${data.calendar_quarter}`;
    } else {
      metaCalendar.textContent = "-";
    }
    metaDate.textContent = data.transcript_date || "-";
    metaCompany.textContent = data.context?.company || data.company || "-";
    metaSector.textContent = data.context?.sector || data.sector || "-";
    renderAgentic(data.agentic_result);
    debugJson.textContent = JSON.stringify(data, null, 2);
    lastAnalysisResult = data;

    // KPI + 詳細資訊
    const agent = data.agentic_result || {};
    const rawNotes = (agent.raw && agent.raw.notes) || {};
    const tokenUsage = data.token_usage || agent.raw?.token_usage || {};
    const postReturn = data.post_return_meta?.return ?? data.post_earnings_return;

    kpiPred.textContent = agent.prediction || "N/A";
    kpiConf.textContent =
      agent.confidence != null ? `${Math.round((agent.confidence || 0) * 100)}%` : "-";
    kpiReturn.textContent = postReturn != null ? `${(postReturn * 100).toFixed(2)}%` : "N/A";
    const cost = tokenUsage.cost_usd != null ? `$${tokenUsage.cost_usd.toFixed(4)}` : "N/A";
    kpiCost.textContent = `${cost}`;
    kpiReturn.classList.remove("pos", "neg");
    if (postReturn != null) {
      if (postReturn > 0) kpiReturn.classList.add("pos");
      else if (postReturn < 0) kpiReturn.classList.add("neg");
    }

    // Earnings Backtest display (T+30)
    const backtestSession = document.getElementById("backtest-session");
    const backtestFrom = document.getElementById("backtest-from");
    const backtestTo = document.getElementById("backtest-to");
    const backtestChange = document.getElementById("backtest-change");
    const bt = data.backtest;
    if (bt && backtestSession && backtestFrom && backtestTo && backtestChange) {
      const sessionLabel = bt.session === "BMO" ? "盤前 (BMO)" : bt.session === "AMC" ? "盤後 (AMC)" : "-";
      backtestSession.textContent = `${bt.earnings_date || "-"} ${sessionLabel}`;
      const fmtPrice = (p) => (p != null ? `$${p.toFixed(2)}` : "N/A");
      backtestFrom.textContent = bt.from_date ? `T+1: ${bt.from_date} ${fmtPrice(bt.from_close)}` : "-";
      backtestTo.textContent = bt.to_date ? `T+30: ${bt.to_date} ${fmtPrice(bt.to_close)}` : "-";
      if (bt.change_pct != null) {
        const sign = bt.change_pct > 0 ? "+" : "";
        backtestChange.textContent = `${sign}${bt.change_pct.toFixed(2)}%`;
        backtestChange.classList.remove("pos", "neg");
        if (bt.change_pct > 0) backtestChange.classList.add("pos");
        else if (bt.change_pct < 0) backtestChange.classList.add("neg");
      } else {
        backtestChange.textContent = "N/A";
        backtestChange.classList.remove("pos", "neg");
      }
    } else if (backtestSession && backtestFrom && backtestTo && backtestChange) {
      backtestSession.textContent = "-";
      backtestFrom.textContent = "-";
      backtestTo.textContent = "-";
      backtestChange.textContent = "-";
      backtestChange.classList.remove("pos", "neg");
    }

    const clean = (val) => {
      if (!val) return null;
      const s = String(val).trim();
      return ["n/a", "na", "none"].includes(s.toLowerCase()) ? null : s;
    };
    detailCmp.textContent = clean(rawNotes.peers) || "尚未產生（可能缺少 Neo4j 資料）";
    detailHist.textContent = clean(rawNotes.past) || "尚未產生（可能缺少 Neo4j 資料）";
    detailPerf.textContent = clean(rawNotes.financials) || "尚未產生（可能缺少 Neo4j 資料）";
    detailBaseline.textContent = "Baseline / Sentiment 預留";
    const tokensToShow =
      tokenUsage.cost_usd != null ? { cost_usd: tokenUsage.cost_usd, ...tokenUsage } : tokenUsage;
    detailTokens.textContent = Object.keys(tokensToShow || {}).length
      ? JSON.stringify(tokensToShow, null, 2)
      : "N/A";

    const summarize = (txt) => {
      if (!txt) return "尚無資料";
      const words = String(txt).split(/\s+/);
      if (words.length <= 12) return txt;
      return words.slice(0, 12).join(" ") + "...";
    };
    detailCmpSummary.textContent = summarize(detailCmp.textContent);
    detailHistSummary.textContent = summarize(detailHist.textContent);
    detailPerfSummary.textContent = summarize(detailPerf.textContent);
    detailBaselineSummary.textContent = summarize(detailBaseline.textContent);
    detailTokensSummary.textContent = summarize(detailTokens.textContent);
    setStatus("分析完成");
    stopAnalysisProgress("分析完成");
  } catch (err) {
    console.error(err);
    setStatus(`分析失敗：${err.message}`, "error");
    stopAnalysisProgress("分析失敗");
  } finally {
    analyzeBtn.disabled = availableDates.length === 0;
    analyzeBtn.textContent = originalLabel;
    if (!progressTimer) stopAnalysisProgress("");
    if (cancelBtn) {
      cancelBtn.disabled = true;
    }
    abortController = null;
  }
}

function collectTextContent(el) {
  if (!el) return "";
  return (el.innerText || el.textContent || "").trim();
}

searchBtn.addEventListener("click", searchSymbols);
searchInput.addEventListener("keyup", (e) => {
  if (e.key === "Enter") searchSymbols();
});
analyzeBtn.addEventListener("click", runAnalysis);
if (cancelBtn) {
  cancelBtn.addEventListener("click", () => {
    if (abortController) {
      abortController.abort();
      setStatus("已停止分析", "error");
      stopAnalysisProgress("已停止分析");
      if (cancelBtn) cancelBtn.disabled = true;
      analyzeBtn.disabled = availableDates.length === 0;
      agenticContent.innerHTML = '<p class="muted">已停止分析。</p>';
    }
  });
}
if (batchBtn) {
  batchBtn.addEventListener("click", runBatch);
}

// Collapsible debug JSON
const debugToggle = document.querySelector(".collapse-toggle");
const debugBody = document.getElementById("debug-json-wrap");
if (debugToggle && debugBody) {
  debugToggle.addEventListener("click", () => {
    const expanded = debugToggle.getAttribute("aria-expanded") === "true";
    debugToggle.setAttribute("aria-expanded", String(!expanded));
    const chevron = debugToggle.querySelector(".chevron");
    if (!expanded) {
      debugBody.hidden = false;
      chevron.style.transform = "rotate(0deg)";
    } else {
      debugBody.hidden = true;
      chevron.style.transform = "rotate(-90deg)";
    }
  });
  // start collapsed
  debugBody.hidden = true;
  const chevron = debugToggle.querySelector(".chevron");
  if (chevron) chevron.style.transform = "rotate(-90deg)";
}

// Init detail toggles
wireDetailToggles();

// 初始狀態
setStatus("輸入公司名稱或代號開始搜尋");
loadEarningsRange(false);
if (todayEarningsQueryBtn) {
  todayEarningsQueryBtn.addEventListener("click", () => {
    loadEarningsRange(true);
  });
}
// Toggle detail sections
function wireDetailToggles() {
  document.querySelectorAll(".detail-toggle").forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-target");
      const body = document.getElementById(targetId);
      const expanded = btn.getAttribute("aria-expanded") === "true";
      btn.setAttribute("aria-expanded", String(!expanded));
      const chevron = btn.querySelector(".chevron");
      if (body) body.hidden = expanded;
      if (chevron) chevron.style.transform = expanded ? "rotate(-90deg)" : "rotate(0deg)";
    });
    // start collapsed
    btn.setAttribute("aria-expanded", "false");
    const chevron = btn.querySelector(".chevron");
    if (chevron) chevron.style.transform = "rotate(-90deg)";
    const targetId = btn.getAttribute("data-target");
    const body = document.getElementById(targetId);
    if (body) body.hidden = true;
  });
}

function stopBatchPolling() {
  if (batchPollTimer) {
    clearInterval(batchPollTimer);
    batchPollTimer = null;
  }
}

async function pollBatch(jobId) {
  if (!jobId) return;
  activeBatchId = jobId;
  const poll = async () => {
    try {
      const res = await fetch(`/api/batch-analyze/${jobId}`);
      if (!res.ok) {
        throw new Error(await res.text());
      }
      const data = await res.json();
      const total = data.total || 0;
      const completed = data.completed || 0;
      const status = data.status || "queued";
      const current = data.current || "";
      const progressTxt = total ? `${completed}/${total}` : `${completed}`;
      const prefix =
        status === "completed"
          ? "完成"
          : status === "failed"
          ? "批次失敗"
          : status === "running"
          ? "處理中"
          : "排隊中";
      const currentTxt = current ? ` | 處理 ${current}` : "";
      batchStatus.innerHTML =
        status === "completed" || status === "failed"
          ? `${prefix}：${progressTxt} 檔`
          : `<span class="spinner"></span> ${prefix}：${progressTxt} 檔${currentTxt}`;
      renderBatch(data.results || []);
      if (status === "completed" || status === "failed") {
        if (status === "failed" && data.error) {
          batchStatus.textContent = `${prefix}：${data.error}`;
        }
        stopBatchPolling();
        return true;
      }
      return false;
    } catch (err) {
      console.error(err);
      batchStatus.textContent = `批次失敗：${err.message}`;
      stopBatchPolling();
      return true;
    }
  };
  const done = await poll();
  stopBatchPolling();
  if (done) return;
  batchPollTimer = setInterval(async () => {
    const finished = await poll();
    if (finished) {
      stopBatchPolling();
    }
  }, 2000);
}

async function runBatch() {
  const raw = batchInput.value || "";
  const tickers = raw
    .split(/[\n,]/)
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean);
  if (!tickers.length) {
    batchStatus.textContent = "請輸入至少一個 ticker";
    return;
  }
  stopBatchPolling();
  batchStatus.innerHTML = `<span class="spinner"></span> 提交批次中...`;
  batchResults.innerHTML = "";
  try {
    const res = await fetch("/api/batch-analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tickers, latest_only: true }),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();
    if (!data.job_id) {
      throw new Error("missing job_id");
    }
    batchStatus.innerHTML = `<span class="spinner"></span> 已提交，Job ${data.job_id}，排隊中...`;
    await pollBatch(data.job_id);
  } catch (err) {
    console.error(err);
    batchStatus.textContent = `批次失敗：${err.message}`;
  }
}

function renderBatch(rows) {
  if (!rows || !rows.length) {
    batchResults.innerHTML = '<p class="muted small">無結果</p>';
    return;
  }
  const header = `<div class="row header"><div>Ticker</div><div>狀態</div><div>Prediction</div><div>Conf.</div><div>Post Ret</div></div>`;
  const body = rows
    .map((r) => {
      const agent = (r.payload && r.payload.agentic_result) || {};
      const pred = agent.prediction || "-";
      const conf = agent.confidence != null ? `${Math.round((agent.confidence || 0) * 100)}%` : "-";
      const post = r.payload ? r.payload.post_earnings_return : null;
      const postTxt = post != null ? `${(post * 100).toFixed(2)}%` : "N/A";
      const badge = r.status === "ok" ? '<span class="badge">OK</span>' : '<span class="badge" style="background: rgba(220,38,38,0.15); color:#dc2626; border-color: rgba(220,38,38,0.4);">Error</span>';
      return `<div class="row">
        <div>${r.symbol || "-"}</div>
        <div>${badge} ${r.error || ""}</div>
        <div>${pred}</div>
        <div>${conf}</div>
        <div>${postTxt}</div>
      </div>`;
    })
    .join("");
  batchResults.innerHTML = header + body;
}
