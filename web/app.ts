type PhoneRecord = {
  status: string;
  phone: string;
  name: string;
  age: number;
  gender: string;
  estimated_value: number;
  estimated_equity: number;
  last_sale_amount: number;
  last_sale_date: string;
  email: string;
  ownership_type: string;
  occupancy_type: string;
};

type LookupResult = {
  total: number;
  limit: number;
  rows: PhoneRecord[];
};

type Cell = { text: string; muted: boolean };

const COLUMN_KEYS: (keyof PhoneRecord)[] = [
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
  "occupancy_type",
];

const MONEY_KEYS = new Set<keyof PhoneRecord>([
  "estimated_value",
  "estimated_equity",
  "last_sale_amount",
]);

const PAGE_SIZE = 10;
const moneyFormat = new Intl.NumberFormat("en-US");

const EMPTY_MESSAGE = "🤷‍♂️ 没这号人，老板确认下号码？";
const ERROR_MESSAGE = "💥 后端小哥睡着了，快检查终端！";
const EXPORT_MESSAGE = "🚚 正在打包数据，请勿刷新页面...";
const SEARCH_LABEL = "🔍 正在大海捞针...";
const UPLOAD_MESSAGE = "📤 正在上传并处理...";
const BASE_UPLOAD_MESSAGE = "📤 正在疯狂吞吐底库数据，请勿关闭网页...";
const UPLOAD_OK = "✅ 处理成功！";

function requiredElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) {
    throw new Error(`页面缺少 ${selector}`);
  }
  return element;
}

const form = requiredElement<HTMLFormElement>("#lookup-form");
const summary = requiredElement<HTMLElement>("#summary");
const tbody = requiredElement<HTMLTableSectionElement>("#rows");
const submitButton = requiredElement<HTMLButtonElement>("#submit");
const exportButton = requiredElement<HTMLButtonElement>("#export");
const pager = requiredElement<HTMLElement>("#pager");
const pagePrev = requiredElement<HTMLButtonElement>("#page-prev");
const pageNext = requiredElement<HTMLButtonElement>("#page-next");
const pageNumbers = requiredElement<HTMLElement>("#page-numbers");

let loadedRows: PhoneRecord[] = [];
let page = 1;

function inputValue(id: string): string {
  return requiredElement<HTMLInputElement>(id).value.trim();
}

function currentParams(): URLSearchParams {
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

function setBusy(busy: boolean): void {
  submitButton.disabled = busy;
  exportButton.disabled = busy;
}

function isBlank(value: string): boolean {
  return value.trim() === "";
}

function isUnknownOwner(row: PhoneRecord): boolean {
  return isBlank(row.name)
    && isBlank(row.gender)
    && isBlank(row.email)
    && isBlank(row.last_sale_date)
    && isBlank(row.ownership_type)
    && isBlank(row.occupancy_type)
    && row.age === 0
    && row.estimated_value === 0
    && row.estimated_equity === 0
    && row.last_sale_amount === 0;
}

function formatCell(row: PhoneRecord, key: keyof PhoneRecord): Cell {
  if (key === "name" && isUnknownOwner(row)) {
    return { text: "Unknown (未知号主)", muted: true };
  }
  if (key === "age") {
    return row.age === 0
      ? { text: "—", muted: true }
      : { text: String(row.age), muted: false };
  }
  if (MONEY_KEYS.has(key)) {
    const amount = Number(row[key]);
    return amount === 0
      ? { text: "—", muted: true }
      : { text: `$${moneyFormat.format(amount)}`, muted: false };
  }
  const text = String(row[key] ?? "").trim();
  return text === ""
    ? { text: "—", muted: true }
    : { text, muted: false };
}

function pageCount(): number {
  return Math.max(1, Math.ceil(loadedRows.length / PAGE_SIZE));
}

function renderPager(): void {
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

function statusTone(status: string): string {
  if (status === "触达无效") return "tag-red";
  if (status === "触达有效") return "tag-blue";
  if (status === "触达已开客") return "tag-green";
  return "tag-gray";
}

function renderStatus(status: string): HTMLSpanElement {
  const label = status.trim() || "未触达";
  const tag = document.createElement("span");
  tag.className = `tag ${statusTone(label)}`;
  tag.textContent = label;
  return tag;
}

function renderTable(): void {
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

function showResult(result: LookupResult): void {
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
  summary.textContent = `🎯 捞到了！共找到 ${result.total.toLocaleString("zh-CN")} 条记录，当前展示前 ${shown} 条。`;
  renderTable();
}

function clearResults(): void {
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
    showResult(await response.json() as LookupResult);
  } catch {
    summary.textContent = ERROR_MESSAGE;
  } finally {
    submitButton.textContent = "查询";
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

type EChartsInstance = {
  setOption: (option: object) => void;
  resize: () => void;
};

type EChartsApi = {
  init: (element: HTMLElement) => EChartsInstance;
};

let genderChart: EChartsInstance | null = null;
let ageChart: EChartsInstance | null = null;

function echartsApi(): EChartsApi | null {
  const value = (window as Window & { echarts?: EChartsApi }).echarts;
  return value ?? null;
}

function renderCharts(): void {
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
        { value: 3300, name: "未知", itemStyle: { color: "#475569" } },
      ],
    }],
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
      axisLine: { lineStyle: { color: "rgba(148,163,184,0.35)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#94a3b8" },
      splitLine: { lineStyle: { color: "rgba(148,163,184,0.15)" } },
    },
    series: [{
      type: "bar",
      barWidth: 28,
      data: [8200, 18600, 24100, 19800, 9300],
      itemStyle: {
        borderRadius: [8, 8, 0, 0],
        color: "#22d3ee",
      },
    }],
  });

  window.addEventListener("resize", () => {
    gender.resize();
    age.resize();
  });
}

renderCharts();

const importStatus = requiredElement<HTMLElement>("#import-status");
const uploadButtons = [...document.querySelectorAll<HTMLButtonElement>("[data-upload]")];

function setUploading(busy: boolean): void {
  for (const button of uploadButtons) {
    button.disabled = busy;
  }
}

async function sendUpload(url: string, input: HTMLInputElement): Promise<void> {
  const file = input.files?.[0];
  if (!file) {
    alert(url === "/upload_base" ? "请先选择 CSV 或 TXT 文件" : "请先选择 TXT、CSV 或 Excel 文件");
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
        const payload = await response.json() as { detail?: string };
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
    void sendUpload(url, requiredElement<HTMLInputElement>(selector));
  });
}

const panels: Record<string, HTMLElement> = {
  dashboard: requiredElement("#panel-dashboard"),
  lookup: requiredElement("#panel-lookup"),
  import: requiredElement("#panel-import"),
};
const tabButtons = [...document.querySelectorAll<HTMLButtonElement>("[data-tab]")];

function showPanel(name: string): void {
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
