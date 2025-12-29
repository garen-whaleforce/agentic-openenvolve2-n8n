const statusEl = document.getElementById("status");
const tbody = document.getElementById("calls-body");
const fSymbol = document.getElementById("filter-symbol");
const fSector = document.getElementById("filter-sector");
const fDateFrom = document.getElementById("filter-date-from");
const fDateTo = document.getElementById("filter-date-to");
const fPred = document.getElementById("filter-pred");
const fSort = document.getElementById("filter-sort");
let symbolTypingTimer = null;
const SYMBOL_TYPING_DELAY = 500;

function setStatus(msg) {
  statusEl.textContent = msg;
}

function fmtPct(val) {
  if (val === null || val === undefined) return "-";
  return `${(val * 100).toFixed(2)}%`;
}

function fmtDate(val) {
  if (!val) return "-";
  return val.split("T")[0];
}

async function loadCalls() {
  const params = new URLSearchParams();
  if (fSymbol.value.trim()) params.set("symbol", fSymbol.value.trim());
  if (fSector.value.trim()) params.set("sector", fSector.value.trim());
  if (fDateFrom.value) params.set("date_from", fDateFrom.value);
  if (fDateTo.value) params.set("date_to", fDateTo.value);
  if (fPred.value) params.set("prediction", fPred.value);
  if (fSort.value) params.set("sort", fSort.value);

  setStatus("載入中...");
  tbody.innerHTML = `<tr><td colspan="7" class="muted small">載入中...</td></tr>`;
  try {
    const res = await fetch(`/api/calls?${params.toString()}`);
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    if (!data.length) {
      tbody.innerHTML = `<tr><td colspan="7" class="muted small">沒有資料</td></tr>`;
      setStatus("完成，0 筆");
      return;
    }
    tbody.innerHTML = "";
    data.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.symbol || "-"}</td>
        <td>${fmtDate(row.call_date)}</td>
        <td>${row.sector || "-"}</td>
        <td>${row.post_return != null ? fmtPct(row.post_return) : "-"}</td>
        <td>${row.prediction || "-"}</td>
        <td>${row.confidence != null ? (row.confidence * 100).toFixed(0) + "%" : "-"}</td>
        <td><a class="link-light" href="call_detail.html?job_id=${row.job_id}">詳情</a></td>
      `;
      tbody.appendChild(tr);
    });
    setStatus(`完成，${data.length} 筆`);
  } catch (err) {
    console.error(err);
    setStatus(`載入失敗：${err.message}`);
    tbody.innerHTML = `<tr><td colspan="7" class="muted small">載入失敗</td></tr>`;
  }
}

document.getElementById("filter-run").addEventListener("click", loadCalls);
document.getElementById("filter-reset").addEventListener("click", () => {
  fSymbol.value = "";
  fSector.value = "";
  fDateFrom.value = "";
  fDateTo.value = "";
  fPred.value = "";
  fSort.value = "date_desc";
  loadCalls();
});

fSymbol.addEventListener("input", () => {
  if (symbolTypingTimer) {
    clearTimeout(symbolTypingTimer);
  }
  symbolTypingTimer = setTimeout(() => {
    loadCalls();
  }, SYMBOL_TYPING_DELAY);
});

fSymbol.addEventListener("keyup", (e) => {
  if (e.key === "Enter") {
    if (symbolTypingTimer) clearTimeout(symbolTypingTimer);
    loadCalls();
  }
});

const urlParams = new URLSearchParams(window.location.search);
const initialSymbol = urlParams.get("symbol");
if (initialSymbol) {
  fSymbol.value = initialSymbol;
}

loadCalls();
