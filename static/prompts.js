// ============================================================================
// Prompt Categories Definition
// ============================================================================

const SYSTEM_MESSAGES = [
  "MAIN_AGENT_SYSTEM_MESSAGE",
  "EXTRACTION_SYSTEM_MESSAGE",
  "DELEGATION_SYSTEM_MESSAGE",
  "COMPARATIVE_SYSTEM_MESSAGE",
  "HISTORICAL_EARNINGS_SYSTEM_MESSAGE",
  "FINANCIALS_SYSTEM_MESSAGE",
];

const PROMPT_TEMPLATES = [
  "COMPARATIVE_AGENT_PROMPT",
  "HISTORICAL_EARNINGS_AGENT_PROMPT",
  "FINANCIALS_STATEMENT_AGENT_PROMPT",
  "MAIN_AGENT_PROMPT",
  "FACTS_EXTRACTION_PROMPT",
  "FACTS_DELEGATION_PROMPT",
  "PEER_DISCOVERY_TICKER_PROMPT",
  "MEMORY_PROMPT",
  "BASELINE_PROMPT",
];

// Placeholder hints for each template
const PLACEHOLDER_HINTS = {
  "COMPARATIVE_AGENT_PROMPT": "變數: {{ticker_section}}, {{facts}}, {{related_facts}}",
  "HISTORICAL_EARNINGS_AGENT_PROMPT": "變數: {{fact}}, {{related_facts}}, {{quarter_label}}",
  "FINANCIALS_STATEMENT_AGENT_PROMPT": "變數: {{quarter_label}}, {{fact}}, {{similar_facts}}",
  "MAIN_AGENT_PROMPT": "變數: {{transcript_section}}, {{original_transcript}}, {{financial_statements_section}}, {{qoq_section_str}}, {{notes_financials}}, {{notes_past}}, {{notes_peers}}, {{memory_section}}",
  "FACTS_EXTRACTION_PROMPT": "變數: {{transcript_chunk}}",
  "FACTS_DELEGATION_PROMPT": "變數: {{facts}}",
  "PEER_DISCOVERY_TICKER_PROMPT": "變數: {{ticker}}",
  "MEMORY_PROMPT": "變數: {{all_notes}}, {{actual_return}}",
  "BASELINE_PROMPT": "變數: {{transcript}}",
};

// ============================================================================
// Active Profile Banner
// ============================================================================

async function loadActiveProfileBanner() {
  try {
    const resp = await fetch("/api/prompts/status");
    if (!resp.ok) return;
    const data = await resp.json();
    const profileName = data.active_profile || "default";

    const banner = document.getElementById("profile-status-banner");
    const nameSpan = document.getElementById("active-profile-name");
    nameSpan.textContent = profileName;

    // Update banner class based on profile type
    banner.classList.remove("default", "custom", "profile");
    if (profileName === "default") {
      banner.classList.add("default");
    } else if (profileName === "custom") {
      banner.classList.add("custom");
    } else {
      banner.classList.add("profile");
    }
  } catch (err) {
    console.error("loadActiveProfileBanner error:", err);
  }
}

// ============================================================================
// Profile List
// ============================================================================

async function loadProfileList() {
  const container = document.getElementById("profile-list");
  try {
    const resp = await fetch("/api/prompt_profiles");
    if (!resp.ok) {
      container.innerHTML = "<p>無法載入 profile 列表</p>";
      return;
    }
    const profiles = await resp.json();

    if (profiles.length === 0) {
      container.innerHTML = "<p style='color:#6b7280;'>尚無已儲存的 Profile</p>";
      return;
    }

    container.innerHTML = "";
    profiles.forEach((p) => {
      const item = document.createElement("div");
      item.className = "profile-item";

      const info = document.createElement("div");
      info.innerHTML = `<span>${escapeHtml(p.name)}</span><small>${p.updated_at || ""}</small>`;

      const actions = document.createElement("div");
      actions.className = "profile-item-actions";

      const applyBtn = document.createElement("button");
      applyBtn.textContent = "套用";
      applyBtn.onclick = () => applyProfile(p.name);

      const deleteBtn = document.createElement("button");
      deleteBtn.textContent = "刪除";
      deleteBtn.style.background = "#ef4444";
      deleteBtn.style.color = "#fff";
      deleteBtn.onclick = () => deleteProfile(p.name);

      actions.appendChild(applyBtn);
      actions.appendChild(deleteBtn);

      item.appendChild(info);
      item.appendChild(actions);
      container.appendChild(item);
    });
  } catch (err) {
    console.error("loadProfileList error:", err);
    container.innerHTML = "<p>載入 profile 列表發生錯誤</p>";
  }
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// ============================================================================
// Profile Actions
// ============================================================================

async function applyProfile(name) {
  if (!confirm(`確定要套用 Profile「${name}」嗎？這將覆蓋目前的 prompt 設定。`)) {
    return;
  }
  try {
    const resp = await fetch("/api/prompt_profiles/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      alert("套用失敗：" + (await resp.text()));
      return;
    }
    alert(`已套用 Profile「${name}」`);
    // Reload everything
    await loadPrompts();
    await loadActiveProfileBanner();
  } catch (err) {
    console.error("applyProfile error:", err);
    alert("套用 profile 發生錯誤");
  }
}

async function deleteProfile(name) {
  if (!confirm(`確定要刪除 Profile「${name}」嗎？此動作無法復原。`)) {
    return;
  }
  try {
    const resp = await fetch(`/api/prompt_profiles/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    if (!resp.ok) {
      alert("刪除失敗：" + (await resp.text()));
      return;
    }
    alert(`已刪除 Profile「${name}」`);
    await loadProfileList();
    await loadActiveProfileBanner();
  } catch (err) {
    console.error("deleteProfile error:", err);
    alert("刪除 profile 發生錯誤");
  }
}

async function saveCurrentAsProfile() {
  const input = document.getElementById("new-profile-name");
  const name = (input.value || "").trim();
  if (!name) {
    alert("請輸入 Profile 名稱");
    input.focus();
    return;
  }

  try {
    const resp = await fetch("/api/prompt_profiles", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      alert("儲存失敗：" + (await resp.text()));
      return;
    }
    alert(`已將目前設定儲存為 Profile「${name}」`);
    input.value = "";
    await loadProfileList();
    await loadActiveProfileBanner();
  } catch (err) {
    console.error("saveCurrentAsProfile error:", err);
    alert("儲存 profile 發生錯誤");
  }
}

async function resetToDefault() {
  if (!confirm("確定要恢復所有 prompts 為預設值嗎？")) {
    return;
  }
  try {
    const resp = await fetch("/api/prompt_profiles/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: "default" }),
    });
    if (!resp.ok) {
      alert("恢復失敗：" + (await resp.text()));
      return;
    }
    alert("已恢復為預設值");
    await loadPrompts();
    await loadActiveProfileBanner();
  } catch (err) {
    console.error("resetToDefault error:", err);
    alert("恢復預設值發生錯誤");
  }
}

// ============================================================================
// Prompts List (Grouped by Category)
// ============================================================================

async function loadPrompts() {
  const resp = await fetch("/api/prompts");
  if (!resp.ok) {
    alert("載入 prompts 失敗：" + (await resp.text()));
    return;
  }

  const data = await resp.json();
  const container = document.getElementById("prompt-list");
  container.innerHTML = "";

  // Create a map for quick lookup
  const promptMap = {};
  data.forEach((item) => {
    promptMap[item.key] = item;
  });

  // Create System Messages category
  const systemCategory = document.createElement("div");
  systemCategory.className = "prompt-category";
  const systemHeader = document.createElement("h3");
  systemHeader.className = "system-msg";
  systemHeader.textContent = "System Messages (6 個)";
  systemCategory.appendChild(systemHeader);

  SYSTEM_MESSAGES.forEach((key) => {
    const item = promptMap[key];
    if (item) {
      systemCategory.appendChild(createPromptBlock(item, false));
    }
  });
  container.appendChild(systemCategory);

  // Create Prompt Templates category
  const templateCategory = document.createElement("div");
  templateCategory.className = "prompt-category";
  const templateHeader = document.createElement("h3");
  templateHeader.className = "template";
  templateHeader.textContent = "Prompt Templates (9 個)";
  templateCategory.appendChild(templateHeader);

  PROMPT_TEMPLATES.forEach((key) => {
    const item = promptMap[key];
    if (item) {
      templateCategory.appendChild(createPromptBlock(item, true));
    }
  });
  container.appendChild(templateCategory);
}

function createPromptBlock(item, showPlaceholderHint) {
  const wrapper = document.createElement("div");
  wrapper.className = "prompt-block";

  const label = document.createElement("label");
  label.textContent = item.key;
  label.style.display = "block";
  label.style.fontWeight = "bold";
  label.style.marginBottom = "4px";

  wrapper.appendChild(label);

  // Add placeholder hint for templates
  if (showPlaceholderHint && PLACEHOLDER_HINTS[item.key]) {
    const hint = document.createElement("div");
    hint.className = "placeholder-hint";
    hint.textContent = PLACEHOLDER_HINTS[item.key];
    wrapper.appendChild(hint);
  }

  const textarea = document.createElement("textarea");
  textarea.value = item.content;
  textarea.rows = 16;
  textarea.style.width = "100%";
  textarea.dataset.key = item.key;

  const saveBtn = document.createElement("button");
  saveBtn.textContent = "儲存這個 Prompt";
  saveBtn.style.marginTop = "8px";

  saveBtn.onclick = async () => {
    const payload = {
      key: item.key,
      content: textarea.value,
    };
    const res = await fetch("/api/prompts", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      alert("儲存失敗：" + (await res.text()));
    } else {
      alert("已儲存！");
      // Update banner since status may have changed
      await loadActiveProfileBanner();
    }
  };

  wrapper.appendChild(textarea);
  wrapper.appendChild(saveBtn);

  return wrapper;
}

// ============================================================================
// Event Bindings
// ============================================================================

document.getElementById("back-btn").onclick = () => {
  window.location.href = "/";
};

document.getElementById("btn-reset-default").onclick = resetToDefault;
document.getElementById("btn-save-profile").onclick = saveCurrentAsProfile;

// ============================================================================
// Initialize
// ============================================================================

async function init() {
  try {
    await Promise.all([
      loadActiveProfileBanner(),
      loadProfileList(),
      loadPrompts(),
    ]);
  } catch (err) {
    console.error("init error:", err);
    alert("頁面初始化發生錯誤，請查看 console");
  }
}

init();
