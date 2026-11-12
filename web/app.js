const COLUMN_KEYS = [
  "phone",
  "name",
  "age",
  "gender",
  "estimated_value",
  "estimated_equity",
  "last_sale_amount",
  "last_sale_date",
  "email",
  "ownership_type",
  "occupancy_type"
];
const MONEY_KEYS = /* @__PURE__ */ new Set([
  "estimated_value",
  "estimated_equity",
  "last_sale_amount"
]);
const PAGE_SIZE = 10;
const moneyFormat = new Intl.NumberFormat("en-US");
const EMPTY_MESSAGE = "\u{1F937}\u200D\u2642\uFE0F \u6CA1\u8FD9\u53F7\u4EBA\uFF0C\u8001\u677F\u786E\u8BA4\u4E0B\u53F7\u7801\uFF1F";
const ERROR_MESSAGE = "\u{1F4A5} \u540E\u7AEF\u5C0F\u54E5\u7761\u7740\u4E86\uFF0C\u5FEB\u68C0\u67E5\u7EC8\u7AEF\uFF01";
const EXPORT_MESSAGE = "\u{1F69A} \u6B63\u5728\u6253\u5305\u6570\u636E\uFF0C\u8BF7\u52FF\u5237\u65B0\u9875\u9762...";
const SEARCH_LABEL = "\u{1F50D} \u6B63\u5728\u5927\u6D77\u635E\u9488...";
const UPLOAD_MESSAGE = "\u{1F4E4} \u6B63\u5728\u4E0A\u4F20\u5E76\u5904\u7406...";
const BASE_UPLOAD_MESSAGE = "\u{1F4E4} \u6B63\u5728\u75AF\u72C2\u541E\u5410\u5E95\u5E93\u6570\u636E\uFF0C\u8BF7\u52FF\u5173\u95ED\u7F51\u9875...";
const UPLOAD_OK = "\u2705 \u5904\u7406\u6210\u529F\uFF01";
function requiredElement(selector) {
  const element = document.querySelector(selector);
  if (!element) {
    throw new Error(`\u9875\u9762\u7F3A\u5C11 ${selector}`);
  }
  return element;
}
const form = requiredElement("#lookup-form");
const summary = requiredElement("#summary");
const tbody = requiredElement("#rows");
const submitButton = requiredElement("#submit");
const exportButton = requiredElement("#export");
const pager = requiredElement("#pager");
const pagePrev = requiredElement("#page-prev");
const pageNext = requiredElement("#page-next");
const pageNumbers = requiredElement("#page-numbers");
let loadedRows = [];
let page = 1;
function inputValue(id) {
  return requiredElement(id).value.trim();
}
function currentParams() {
  const params = new URLSearchParams();
  const phone = inputValue("#phone");
  const gender = inputValue("#gender");
  const ageMin = inputValue("#age-min");
  const ageMax = inputValue("#age-max");
  if (phone) params.set("phone", phone);
  if (gender) params.set("gender", gender);
  if (ageMin) params.set("age_min", ageMin);
  if (ageMax) params.set("age_max", ageMax);
  return params;
}
function setBusy(busy) {
  submitButton.disabled = busy;
  exportButton.disabled = busy;
}
function isBlank(value) {
  return value.trim() === "";
}
function isUnknownOwner(row) {
  return isBlank(row.name) && isBlank(row.gender) && isBlank(row.email) && isBlank(row.last_sale_date) && isBlank(row.ownership_type) && isBlank(row.occupancy_type) && row.age === 0 && row.estimated_value === 0 && row.estimated_equity === 0 && row.last_sale_amount === 0;
}
function formatCell(row, key) {
  if (key === "name" && isUnknownOwner(row)) {
    return { text: "Unknown (\u672A\u77E5\u53F7\u4E3B)", muted: true };
  }
  if (key === "age") {
    return row.age === 0 ? { text: "\u2014", muted: true } : { text: String(row.age), muted: false };
  }
  if (MONEY_KEYS.has(key)) {
    const amount = Number(row[key]);
    return amount === 0 ? { text: "\u2014", muted: true } : { text: `$${moneyFormat.format(amount)}`, muted: false };
  }
  const text = String(row[key] ?? "").trim();
  return text === "" ? { text: "\u2014", muted: true } : { text, muted: false };
}
function pageCount() {
  return Math.max(1, Math.ceil(loadedRows.length / PAGE_SIZE));
}
function renderPager() {
  pageNumbers.replaceChildren();
  const pages = pageCount();
  pagePrev.disabled = page <= 1;
  pageNext.disabled = page >= pages;
  for (let number = 1; number <= pages; number += 1) {
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.page = String(number);
    button.textContent = String(number);
    if (number === page) {
      button.setAttribute("aria-current", "page");
    }
    pageNumbers.append(button);
  }
}
function statusTone(status) {
  if (status === "\u89E6\u8FBE\u65E0\u6548") return "tag-red";
  if (status === "\u89E6\u8FBE\u6709\u6548") return "tag-blue";
  if (status === "\u89E6\u8FBE\u5DF2\u5F00\u5BA2") return "tag-green";
  return "tag-gray";
}
function renderStatus(status) {
  const label = status.trim() || "\u672A\u89E6\u8FBE";
  const tag = document.createElement("span");
  tag.className = `tag ${statusTone(label)}`;
  tag.textContent = label;
  return tag;
}
function renderTable() {
  tbody.replaceChildren();
  if (loadedRows.length === 0) {
    pager.hidden = true;
    return;
  }
  pager.hidden = false;
  const start = (page - 1) * PAGE_SIZE;
  for (const row of loadedRows.slice(start, start + PAGE_SIZE)) {
    const tr = document.createElement("tr");
    const statusCell = document.createElement("td");
    statusCell.append(renderStatus(row.status));
    tr.append(statusCell);
    for (const key of COLUMN_KEYS) {
      const td = document.createElement("td");
      const cell = formatCell(row, key);
      td.textContent = cell.text;
      if (cell.muted) {
        td.classList.add("muted");
      }
      tr.append(td);
    }
    tbody.append(tr);
  }
  renderPager();
}
function showResult(result) {
  if (result.total === 0 || result.rows.length === 0) {
    loadedRows = [];
    page = 1;
    summary.textContent = EMPTY_MESSAGE;
    renderTable();
    return;
  }
  loadedRows = result.rows;
  page = 1;
  const shown = Math.min(result.rows.length, 100);
  summary.textContent = `\u{1F3AF} \u635E\u5230\u4E86\uFF01\u5171\u627E\u5230 ${result.total.toLocaleString("zh-CN")} \u6761\u8BB0\u5F55\uFF0C\u5F53\u524D\u5C55\u793A\u524D ${shown} \u6761\u3002`;
  renderTable();
}
function clearResults() {
  loadedRows = [];
  page = 1;
  summary.textContent = "";
  renderTable();
}
form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const params = currentParams();
  setBusy(true);
  submitButton.textContent = SEARCH_LABEL;
  clearResults();
  try {
    const response = await fetch(`/lookup?${params.toString()}`);
    if (!response.ok) {
      summary.textContent = ERROR_MESSAGE;
      return;
    }
    showResult(await response.json());
  } catch {
    summary.textContent = ERROR_MESSAGE;
  } finally {
    submitButton.textContent = "\u67E5\u8BE2";
    setBusy(false);
  }
});
exportButton.addEventListener("click", async () => {
  setBusy(true);
  summary.textContent = EXPORT_MESSAGE;
  try {
    const response = await fetch(`/export?${currentParams().toString()}`);
    if (!response.ok) {
      summary.textContent = ERROR_MESSAGE;
      return;
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "phone_records.csv";
    link.click();
    URL.revokeObjectURL(url);
  } catch {
    summary.textContent = ERROR_MESSAGE;
  } finally {
    setBusy(false);
  }
});
pagePrev.addEventListener("click", () => {
  if (page <= 1) return;
  page -= 1;
  renderTable();
});
pageNext.addEventListener("click", () => {
  if (page >= pageCount()) return;
  page += 1;
  renderTable();
});
pageNumbers.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement) || !target.dataset.page) return;
  page = Number(target.dataset.page);
  renderTable();
});
let genderChart = null;
let ageChart = null;
function echartsApi() {
  const value = window.echarts;
  return value ?? null;
}
function renderCharts() {
  const charts = echartsApi();
  if (!charts) return;
  const gender = charts.init(requiredElement("#chart-gender"));
  genderChart = gender;
  gender.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "item" },
    legend: { bottom: 0, textStyle: { color: "#cbd5e1" } },
    series: [{
      type: "pie",
      radius: ["48%", "72%"],
      center: ["50%", "44%"],
      label: { color: "#e2e8f0", formatter: "{b}\n{d}%" },
      data: [
        { value: 41280, name: "Female", itemStyle: { color: "#22d3ee" } },
        { value: 35420, name: "Male", itemStyle: { color: "#6366f1" } },
        { value: 3300, name: "\u672A\u77E5", itemStyle: { color: "#475569" } }
      ]
    }]
  });
  const age = charts.init(requiredElement("#chart-age"));
  ageChart = age;
  age.setOption({
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    grid: { left: 48, right: 12, top: 24, bottom: 28 },
    xAxis: {
      type: "category",
      data: ["18-30", "31-45", "46-60", "61-75", "76+"],
      axisLabel: { color: "#94a3b8" },
      axisLine: { lineStyle: { color: "rgba(148,163,184,0.35)" } }
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#94a3b8" },
      splitLine: { lineStyle: { color: "rgba(148,163,184,0.15)" } }
    },
    series: [{
      type: "bar",
      barWidth: 28,
      data: [8200, 18600, 24100, 19800, 9300],
      itemStyle: {
        borderRadius: [8, 8, 0, 0],
        color: "#22d3ee"
      }
    }]
  });
  window.addEventListener("resize", () => {
    gender.resize();
    age.resize();
  });
}
renderCharts();
const importStatus = requiredElement("#import-status");
const uploadButtons = [...document.querySelectorAll("[data-upload]")];
function setUploading(busy) {
  for (const button of uploadButtons) {
    button.disabled = busy;
  }
}
async function sendUpload(url, input) {
  const file = input.files?.[0];
  if (!file) {
    alert(url === "/upload_base" ? "\u8BF7\u5148\u9009\u62E9 CSV \u6216 TXT \u6587\u4EF6" : "\u8BF7\u5148\u9009\u62E9 TXT\u3001CSV \u6216 Excel \u6587\u4EF6");
    return;
  }
  const body = new FormData();
  body.append("file", file);
  setUploading(true);
  importStatus.textContent = url === "/upload_base" ? BASE_UPLOAD_MESSAGE : UPLOAD_MESSAGE;
  try {
    const response = await fetch(url, { method: "POST", body });
    if (!response.ok) {
      let message = ERROR_MESSAGE;
      try {
        const payload = await response.json();
        if (typeof payload.detail === "string" && payload.detail) {
          message = payload.detail;
        }
      } catch {
        message = ERROR_MESSAGE;
      }
      importStatus.textContent = "";
      alert(message);
      return;
    }
    importStatus.textContent = "";
    alert(UPLOAD_OK);
    input.value = "";
  } catch {
    importStatus.textContent = "";
    alert(ERROR_MESSAGE);
  } finally {
    setUploading(false);
  }
}
for (const button of uploadButtons) {
  button.addEventListener("click", () => {
    const selector = button.dataset.upload;
    const url = button.dataset.url;
    if (!selector || !url) return;
    void sendUpload(url, requiredElement(selector));
  });
}
const panels = {
  dashboard: requiredElement("#panel-dashboard"),
  lookup: requiredElement("#panel-lookup"),
  import: requiredElement("#panel-import")
};
const tabButtons = [...document.querySelectorAll("[data-tab]")];
function showPanel(name) {
  for (const button of tabButtons) {
    if (button.dataset.tab === name) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
  }
  for (const [key, panel] of Object.entries(panels)) {
    panel.hidden = key !== name;
  }
  if (name === "dashboard") {
    requestAnimationFrame(() => {
      genderChart?.resize();
      ageChart?.resize();
    });
  }
}
for (const button of tabButtons) {
  button.addEventListener("click", () => {
    const name = button.dataset.tab;
    if (!name || !(name in panels)) return;
    showPanel(name);
  });
}
showPanel("dashboard");
