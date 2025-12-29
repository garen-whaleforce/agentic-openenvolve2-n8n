const params = new URLSearchParams(window.location.search);
const jobId = params.get("job_id");

const elTitle = document.getElementById("title");
const metaSymbol = document.getElementById("meta-symbol");
const metaCompany = document.getElementById("meta-company");
const metaSector = document.getElementById("meta-sector");
const metaFiscal = document.getElementById("meta-fiscal");
const metaCalendar = document.getElementById("meta-calendar");
const metaDate = document.getElementById("meta-date");
const kpiPred = document.getElementById("kpi-pred");
const kpiConf = document.getElementById("kpi-conf");
const kpiReturn = document.getElementById("kpi-return");
const kpiCost = document.getElementById("kpi-cost");
const agenticContent = document.getElementById("agentic-content");
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
const debugJson = document.getElementById("raw-json");

if (!jobId) {
  elTitle.textContent = "缺少 job_id";
} else {
  loadDetail(jobId);
}

function fmtPct(val) {
  if (val === null || val === undefined) return "-";
  return `${(val * 100).toFixed(2)}%`;
}

function summarize(txt) {
  if (!txt) return "尚無資料";
  const words = String(txt).split(/\s+/);
  if (words.length <= 12) return txt;
  return words.slice(0, 12).join(" ") + "...";
}

async function loadDetail(id) {
  elTitle.textContent = "載入中...";
  try {
    const res = await fetch(`/api/call/${id}`);
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    elTitle.textContent = `${data.symbol || ""} 詳細分析`;

    // Meta + KPI
    metaSymbol.textContent = data.symbol || "-";
    metaCompany.textContent = data.company || "-";
    metaSector.textContent = data.sector || "-";
    metaFiscal.textContent =
      data.fiscal_year && data.fiscal_quarter ? `FY${data.fiscal_year} Q${data.fiscal_quarter}` : "-";
    metaCalendar.textContent = data.call_date ? data.call_date : "-";
    metaDate.textContent = data.call_date || "-";

    const postReturn = data.post_return;
    const agent = data.agent_result || {};
    const pred = agent.prediction || data.prediction || "-";
    const conf = agent.confidence != null ? agent.confidence : data.confidence;
    const tokenUsage = data.token_usage || agent.raw?.token_usage || {};
    kpiPred.textContent = pred;
    kpiConf.textContent = conf != null ? `${Math.round(conf * 100)}%` : "-";
    kpiReturn.textContent = postReturn != null ? fmtPct(postReturn) : "未計算";
    const cost = tokenUsage.cost_usd != null ? `$${tokenUsage.cost_usd.toFixed(4)}` : "N/A";
    kpiCost.textContent = cost;

    renderAgentic(agent);

    const notes = agent.raw?.notes || {};
    const clean = (val) => {
      if (!val) return null;
      const s = String(val).trim();
      return ["n/a", "na", "none"].includes(s.toLowerCase()) ? null : s;
    };

    detailCmp.textContent = clean(notes.peers) || "尚未產生（可能缺少 Neo4j 資料）";
    detailHist.textContent = clean(notes.past) || "尚未產生（可能缺少 Neo4j 資料）";
    detailPerf.textContent = clean(notes.financials) || "尚未產生（可能缺少 Neo4j 資料）";
    detailBaseline.textContent = "Baseline 尚未接入（預留欄位）";

    const tokensToShow = tokenUsage.cost_usd != null ? { cost_usd: tokenUsage.cost_usd, ...tokenUsage } : tokenUsage;
    detailTokens.textContent = Object.keys(tokensToShow || {}).length
      ? JSON.stringify(tokensToShow, null, 2)
      : "N/A";

    detailCmpSummary.textContent = summarize(detailCmp.textContent);
    detailHistSummary.textContent = summarize(detailHist.textContent);
    detailPerfSummary.textContent = summarize(detailPerf.textContent);
    detailBaselineSummary.textContent = summarize(detailBaseline.textContent);
    detailTokensSummary.textContent = summarize(detailTokens.textContent);

    debugJson.textContent = JSON.stringify(data, null, 2);

    wireDetailToggles();
    wireAccordion(agenticContent);
    wireDebugCollapse();
  } catch (err) {
    console.error(err);
    elTitle.textContent = `載入失敗：${err.message}`;
  }
}

function renderAgentic(result) {
  if (!result) {
    agenticContent.innerHTML = '<p class="muted">尚未執行分析。</p>';
    return;
  }
  const { prediction, confidence, summary, reasons, next_steps, metadata } = result;
  const confidenceBadge = confidence != null ? `<span class="badge">信心度 ${(confidence * 100).toFixed(0)}%</span>` : "";
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
      ? `<h4>理由</h4><div class="accordion">${reasons
          .map((r, idx) => {
            const summaryTxt =
              typeof r === "string"
                ? r.split(" ").slice(0, 8).join(" ") + (r.split(" ").length > 8 ? "..." : "")
                : "理由";
            return `
            <div class="accordion-item">
              <button class="accordion-header" type="button" aria-expanded="false" data-target="reason-${idx}">
                <span>${summaryTxt}</span>
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

  agenticContent.innerHTML = `
    <h4>${prediction || "N/A"} ${confidenceBadge}</h4>
    <p>${summary || ""}</p>
    ${reasonsMarkup}
    ${nextStepsMarkup}
  `;
}

function wireAccordion(root) {
  const reasonHeaders = root.querySelectorAll(".accordion-header");
  reasonHeaders.forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-target");
      const body = root.querySelector(`#${targetId}`);
      const expanded = btn.getAttribute("aria-expanded") === "true";
      btn.setAttribute("aria-expanded", String(!expanded));
      if (body) {
        body.hidden = expanded;
        const chev = btn.querySelector(".chevron");
        if (chev) chev.style.transform = expanded ? "rotate(-90deg)" : "rotate(0deg)";
      }
    });
    btn.setAttribute("aria-expanded", "false");
    const chev = btn.querySelector(".chevron");
    if (chev) chev.style.transform = "rotate(-90deg)";
    const targetId = btn.getAttribute("data-target");
    const body = root.querySelector(`#${targetId}`);
    if (body) body.hidden = true;
  });
}

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
    btn.setAttribute("aria-expanded", "false");
    const chevron = btn.querySelector(".chevron");
    if (chevron) chevron.style.transform = "rotate(-90deg)";
    const targetId = btn.getAttribute("data-target");
    const body = document.getElementById(targetId);
    if (body) body.hidden = true;
  });
}

function wireDebugCollapse() {
  const debugToggle = document.querySelector(".collapse-toggle");
  const debugBody = document.getElementById("debug-json-wrap");
  if (debugToggle && debugBody) {
    debugToggle.addEventListener("click", () => {
      const expanded = debugToggle.getAttribute("aria-expanded") === "true";
      debugToggle.setAttribute("aria-expanded", String(!expanded));
      const chevron = debugToggle.querySelector(".chevron");
      if (!expanded) {
        debugBody.hidden = false;
        if (chevron) chevron.style.transform = "rotate(0deg)";
      } else {
        debugBody.hidden = true;
        if (chevron) chevron.style.transform = "rotate(-90deg)";
      }
    });
    debugBody.hidden = true;
    const chevron = debugToggle.querySelector(".chevron");
    if (chevron) chevron.style.transform = "rotate(-90deg)";
  }
}
