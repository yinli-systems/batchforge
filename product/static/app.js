(() => {
  "use strict";

  const MAX_ITEMS = 500;
  const MAX_ITEM_CHARS = 8000;
  const POLL_MS = 2000;
  const RETRY_MS = 5000;
  const CLAMP_CHARS = 280;
  const RECENT_KEY = "batch-text-jobs:recent";
  const TEMPLATE_KEY = "batch-text-jobs:template";
  const TERMINAL = new Set(["completed", "failed", "cancelled"]);
  const ID_PATTERN = /^[A-Za-z0-9_-]{1,100}$/;

  const EXAMPLES = {
    sentiment: [
      "The delivery was three days late, but the driver was kind and apologised.",
      "Setup took two minutes and it has worked every day since.",
      "Support never answered my three emails. Very disappointing.",
      "It is fine. Nothing special, nothing wrong.",
    ],
    summary: [
      "Our quarterly review found that support tickets fell by 18 percent after the self-service portal launched in March. Average resolution time improved from two days to nine hours, although customers in the enterprise tier still report longer waits during peak weeks.",
      "The city council approved a plan to add protected bike lanes along three main roads. Construction will begin next spring and is expected to take eighteen months, with the first section opening to riders before the winter.",
    ],
    extract: [
      "Invoice 4471 dated 3 March 2026 for $1,250 from Northwind Traders.",
      "Meeting moved to Thursday 14 May, budget review, approx. €8,000.",
      "Quick note about the weather, nothing to extract here.",
    ],
  };

  const $ = (id) => document.getElementById(id);
  const n = (value) => Number(value || 0).toLocaleString();

  const state = {
    templates: [],
    template: null,
    jobId: null,
    job: null,
    rows: [],
    filter: "all",
    expanded: new Set(),
    pollTimer: null,
    pollGeneration: 0,
    busy: false,
  };

  /* ---------- storage: per-browser conveniences, every access may fail ---------- */

  function readJSON(key, fallback) {
    try {
      return JSON.parse(localStorage.getItem(key)) ?? fallback;
    } catch {
      return fallback;
    }
  }

  function writeJSON(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      /* Storage can be blocked; the page works without it. */
    }
  }

  function loadRecent() {
    const list = readJSON(RECENT_KEY, []);
    return Array.isArray(list) ? list.filter((entry) => entry && ID_PATTERN.test(entry.id)) : [];
  }

  function saveRecent(list) {
    writeJSON(RECENT_KEY, list.slice(0, 12));
  }

  function rememberJob(entry) {
    saveRecent([entry, ...loadRecent().filter((item) => item.id !== entry.id)]);
  }

  function updateRecent(id, patch) {
    saveRecent(loadRecent().map((item) => (item.id === id ? { ...item, ...patch } : item)));
  }

  /* ---------- helpers ---------- */

  function randomKey() {
    if (window.crypto?.randomUUID) return crypto.randomUUID();
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");
  }

  function ago(seconds) {
    if (!seconds) return "";
    const delta = Math.max(0, Date.now() / 1000 - seconds);
    if (delta < 60) return "just now";
    if (delta < 3600) return `${Math.floor(delta / 60)} min ago`;
    if (delta < 86400) return `${Math.floor(delta / 3600)} h ago`;
    return new Date(seconds * 1000).toLocaleDateString(undefined, { day: "numeric", month: "short" });
  }

  function plural(count, word) {
    return `${n(count)} ${word}${count === 1 ? "" : "s"}`;
  }

  function labelFor(id) {
    return state.templates.find((t) => t.id === id)?.label || "Job";
  }

  function parseLines(text) {
    return text.split("\n").map((line) => line.trim()).filter(Boolean);
  }

  /* Mirrors the server limits so most mistakes are caught before a request. */
  function validate(text) {
    const count = parseLines(text).length;
    if (count === 0) return { count, problem: "" };
    if (count > MAX_ITEMS) {
      return { count, problem: `${n(count)} items is over the limit of ${MAX_ITEMS}. Split the rest into another job.` };
    }
    const longIndex = text.split("\n").findIndex((line) => line.trim().length > MAX_ITEM_CHARS);
    if (longIndex !== -1) {
      return { count, problem: `Line ${longIndex + 1} is longer than ${n(MAX_ITEM_CHARS)} characters.` };
    }
    return { count, problem: "" };
  }

  /* ---------- form ---------- */

  function renderTemplates() {
    const container = $("template-options");
    const saved = readJSON(TEMPLATE_KEY, null);
    state.template = state.templates.some((t) => t.id === saved) ? saved : state.templates[0]?.id;
    container.replaceChildren(...state.templates.map((template) => {
      const label = document.createElement("label");
      label.className = "segment";
      const input = document.createElement("input");
      input.type = "radio";
      input.name = "template";
      input.value = template.id;
      input.checked = template.id === state.template;
      const text = document.createElement("span");
      text.textContent = template.label;
      label.append(input, text);
      return label;
    }));
    container.addEventListener("change", (event) => {
      if (event.target.name !== "template") return;
      state.template = event.target.value;
      writeJSON(TEMPLATE_KEY, state.template);
      showTemplateHelp();
    });
    showTemplateHelp();
  }

  function showTemplateHelp() {
    const template = state.templates.find((t) => t.id === state.template);
    $("template-help").textContent = template?.description || "";
  }

  function updateForm() {
    const { count, problem } = validate($("text").value);
    $("text-count").textContent = plural(count, "item");
    $("text-problem").textContent = problem;
    $("text-problem").hidden = !problem;
    $("text").setAttribute("aria-invalid", problem ? "true" : "false");
    $("submit").disabled = state.busy || count === 0 || Boolean(problem);
  }

  function loadExample() {
    const examples = EXAMPLES[state.template] || [];
    if (!examples.length) return;
    if ($("text").value.trim() && !window.confirm("Replace what you have entered with the example?")) return;
    $("text").value = examples.join("\n");
    updateForm();
    $("text").focus();
  }

  function setBusy(busy) {
    state.busy = busy;
    $("submit").textContent = busy ? "Starting…" : "Run job";
    updateForm();
  }

  async function api(path, options = {}) {
    let response;
    try {
      response = await fetch(path, options);
    } catch {
      throw new Error("Can't reach the server. Check your connection and try again.");
    }
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(body?.error?.message || `Something went wrong (HTTP ${response.status}).`);
      error.status = response.status;
      throw error;
    }
    return body;
  }

  async function submit() {
    const { count, problem } = validate($("text").value);
    if (!count || problem || state.busy) return;
    setBusy(true);
    showNotice("form-error", "");
    try {
      const job = await api("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          template: state.template,
          text: $("text").value,
          idempotency_key: randomKey(),
        }),
      });
      rememberJob({ id: job.id, template: job.template, created: job.created_at, total: job.total });
      renderRecent();
      $("text").value = "";
      setBusy(false);
      openJob(job.id);
    } catch (error) {
      showNotice("form-error", error.message);
      setBusy(false);
    }
  }

  function showNotice(id, message, { retry = null } = {}) {
    const el = $(id);
    el.replaceChildren();
    el.hidden = !message;
    if (!message) return;
    el.append(document.createTextNode(message));
    if (retry) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "link";
      button.textContent = "Try again";
      button.addEventListener("click", retry);
      el.append(button);
    }
  }

  /* ---------- job view ---------- */

  function statusFor(job) {
    if (job.status === "in_progress") return { key: "running", label: "Running" };
    if (job.status === "cancelled") return { key: "cancelled", label: "Cancelled" };
    if (job.status === "failed" || (job.completed === 0 && job.failed > 0)) return { key: "failed", label: "Failed" };
    if (job.failed > 0) return { key: "partial", label: "Finished with errors" };
    return { key: "completed", label: "Completed" };
  }

  function renderJob(job) {
    state.job = job;
    const { key, label } = statusFor(job);
    $("job-title").textContent = labelFor(job.template);
    $("job-status").className = `status ${key}`;
    $("job-status-text").textContent = label;

    const pct = Math.round(job.progress * 100);
    $("progress").setAttribute("aria-valuenow", String(pct));
    $("progress-fill").style.width = `${pct}%`;
    $("progress-fill").classList.toggle("done", TERMINAL.has(job.status) && key === "completed");

    const finished = TERMINAL.has(job.status);
    $("progress-label").textContent = progressText(job, finished);

    const tokens = (job.usage?.prompt_tokens || 0) + (job.usage?.completion_tokens || 0);
    $("job-meta").textContent = [
      job.id,
      plural(job.total, "item"),
      tokens ? `${n(tokens)} tokens` : "",
      job.created_at ? `started ${ago(job.created_at)}` : "",
    ].filter(Boolean).join(" · ");

    updateRecent(job.id, { status: job.status, completed: job.completed, failed: job.failed, total: job.total });
    renderRecent();
    renderDownload(job);
    renderResultsArea(finished);
  }

  /* Once finished, the rows say what the user actually sees: outputs that need a look are not "done". */
  function progressText(job, finished) {
    const rows = job.rows || [];
    const count = (status) => rows.filter((row) => row.status === status).length;
    if (!finished) {
      const parts = [`${n(job.completed)} done`];
      if (job.failed) parts.push(`${n(job.failed)} failed`);
      parts.push(`${n(job.pending)} waiting`);
      return `${n(job.completed + job.failed)} of ${n(job.total)} · ${parts.join(" · ")}`;
    }
    const parts = [`${n(rows.length ? count("completed") : job.completed)} done`];
    if (count("invalid_json")) parts.push(`${n(count("invalid_json"))} to check`);
    const failed = rows.length ? count("failed") : job.failed;
    if (failed) parts.push(`${n(failed)} failed`);
    return parts.join(" · ");
  }

  function renderDownload(job) {
    const link = $("download");
    const ready = TERMINAL.has(job.status) && job.total > 0;
    link.setAttribute("aria-disabled", ready ? "false" : "true");
    link.tabIndex = ready ? 0 : -1;
    if (ready) {
      link.href = `/api/jobs/${encodeURIComponent(job.id)}/export.csv`;
      link.setAttribute("download", "");
    } else {
      link.removeAttribute("href");
      link.removeAttribute("download");
    }
  }

  function renderResultsArea(finished) {
    $("waiting").hidden = finished;
    $("results").hidden = !finished;
  }

  const FILTERS = [
    { id: "all", label: "All", match: () => true },
    { id: "completed", label: "Done", match: (r) => r.status === "completed" },
    { id: "failed", label: "Failed", match: (r) => r.status === "failed" },
    { id: "invalid_json", label: "Check output", match: (r) => r.status === "invalid_json" },
  ];

  function renderFilters() {
    const counts = {};
    for (const row of state.rows) counts[row.status] = (counts[row.status] || 0) + 1;
    $("filters").replaceChildren(...FILTERS.filter((filter) => {
      if (filter.id === "all" || filter.id === state.filter) return true;
      return (counts[filter.id] || 0) > 0;
    }).map((filter) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "filter";
      button.setAttribute("aria-pressed", String(state.filter === filter.id));
      button.textContent = filter.label;
      const count = document.createElement("span");
      count.className = "n";
      count.textContent = n(filter.id === "all" ? state.rows.length : counts[filter.id] || 0);
      button.append(count);
      button.addEventListener("click", () => {
        state.filter = filter.id;
        renderFilters();
        renderRows();
      });
      return button;
    }));
  }

  /* Result text depends on the task: sentiment becomes a label, extraction becomes fields. */
  function resultContent(row, template) {
    const cell = document.createElement("td");
    cell.className = "result";

    if (row.status === "failed") {
      cell.classList.add("is-muted");
      const title = document.createElement("span");
      title.className = "failed-text";
      title.textContent = "Couldn't process this item";
      const reason = document.createElement("span");
      reason.className = "sub";
      reason.textContent = row.error || "";
      cell.append(title, reason);
      return cell;
    }
    if (row.status === "pending") {
      cell.classList.add("is-muted");
      cell.textContent = "Waiting";
      return cell;
    }
    if (row.status === "invalid_json") {
      const title = document.createElement("span");
      title.className = "label";
      title.textContent = "The output wasn't valid JSON";
      const raw = document.createElement("span");
      raw.className = "sub";
      raw.textContent = row.text;
      cell.append(title, raw);
      return cell;
    }

    if (template === "sentiment") {
      const word = row.text.toLowerCase().split(/\s+/)[0].replace(/[^a-z]/g, "");
      const kind = ["positive", "negative", "neutral"].includes(word) ? word : "neutral";
      const span = document.createElement("span");
      span.className = `sent ${kind}`;
      span.textContent = row.text.toLowerCase();
      cell.append(span);
      return cell;
    }

    if (template === "extract") {
      let fields;
      try {
        fields = JSON.parse(row.text);
      } catch {
        cell.textContent = row.text;
        return cell;
      }
      const list = document.createElement("dl");
      list.className = "fields";
      for (const [name, value] of Object.entries(fields)) {
        const term = document.createElement("dt");
        term.textContent = name;
        const detail = document.createElement("dd");
        if (value === null || value === "") {
          detail.className = "none";
          detail.textContent = "—";
        } else {
          detail.textContent = String(value);
        }
        list.append(term, detail);
      }
      cell.append(list);
      return cell;
    }

    const expanded = state.expanded.has(row.custom_id);
    const long = row.text.length > CLAMP_CHARS;
    cell.textContent = long && !expanded ? `${row.text.slice(0, CLAMP_CHARS).trimEnd()}…` : row.text;
    if (long) {
      const toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "more";
      toggle.textContent = expanded ? "Show less" : "Show more";
      toggle.addEventListener("click", () => {
        if (expanded) state.expanded.delete(row.custom_id);
        else state.expanded.add(row.custom_id);
        renderRows();
      });
      cell.append(toggle);
    }
    return cell;
  }

  function renderRows() {
    const job = state.job;
    const template = job?.template;
    const filter = FILTERS.find((f) => f.id === state.filter) || FILTERS[0];
    const rows = state.rows.filter(filter.match);

    $("rows").replaceChildren(...rows.map((row) => {
      const tr = document.createElement("tr");
      const input = document.createElement("td");
      input.className = "input";
      input.textContent = row.input;
      tr.append(input, resultContent(row, template));
      return tr;
    }));

    const empty = $("rows-empty");
    empty.textContent = state.rows.length === 0 ? "This job has no items." : "Nothing matches this filter.";
    empty.hidden = rows.length > 0;
  }

  function renderResults(rows) {
    state.rows = rows;
    renderFilters();
    renderRows();
  }

  function stopPolling() {
    clearTimeout(state.pollTimer);
    state.pollTimer = null;
    state.pollGeneration += 1;
  }

  async function poll(id, generation) {
    if (generation !== state.pollGeneration) return;
    try {
      const job = await api(`/api/jobs/${encodeURIComponent(id)}`);
      if (generation !== state.pollGeneration) return;
      showNotice("job-notice", "");
      renderJob(job);
      renderResults(job.rows || []);
      if (!TERMINAL.has(job.status)) {
        state.pollTimer = setTimeout(() => poll(id, generation), POLL_MS);
      }
    } catch (error) {
      if (generation !== state.pollGeneration) return;
      if (error.status === 404) {
        stopPolling();
        $("job-status").className = "status failed";
        $("job-status-text").textContent = "Not found";
        showNotice("job-notice", "This job isn't available. It may have expired or belong to another account.");
        return;
      }
      showNotice("job-notice", `${error.message} Still trying.`, { retry: () => {
        stopPolling();
        poll(id, state.pollGeneration);
      } });
      state.pollTimer = setTimeout(() => poll(id, generation), RETRY_MS);
    }
  }

  function openJob(id) {
    if (!ID_PATTERN.test(id)) return;
    stopPolling();
    state.jobId = id;
    state.job = null;
    state.rows = [];
    state.filter = "all";
    state.expanded = new Set();

    $("job").hidden = false;
    $("job-title").textContent = labelFor(loadRecent().find((item) => item.id === id)?.template);
    $("job-meta").textContent = id;
    $("job-status").className = "status running";
    $("job-status-text").textContent = "Loading";
    $("progress-fill").style.width = "0%";
    $("progress-label").textContent = "";
    $("download").setAttribute("aria-disabled", "true");
    $("download").removeAttribute("href");
    $("waiting").hidden = false;
    $("results").hidden = true;
    $("rows").replaceChildren();
    $("filters").replaceChildren();
    showNotice("job-notice", "");

    if (location.hash !== `#job=${id}`) history.replaceState(null, "", `#job=${id}`);
    $("job").scrollIntoView({ behavior: "smooth", block: "start" });
    poll(id, state.pollGeneration);
  }

  /* ---------- recent ---------- */

  function renderRecent() {
    const list = loadRecent();
    $("recent").hidden = list.length === 0;
    $("recent-card").hidden = list.length === 0;
    $("recent").replaceChildren(...list.map((entry) => {
      const item = document.createElement("li");

      const main = document.createElement("div");
      const link = document.createElement("a");
      link.href = `#job=${entry.id}`;
      link.textContent = labelFor(entry.template);
      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = [
        entry.total !== undefined ? plural(entry.total, "item") : "",
        entry.created ? ago(entry.created) : "",
      ].filter(Boolean).join(" · ");
      main.append(link, meta);

      const end = document.createElement("div");
      end.className = "end";
      const status = entry.status ? statusFor(entry).label : "Running";
      const label = document.createElement("span");
      label.textContent = status;
      const forget = document.createElement("button");
      forget.type = "button";
      forget.className = "forget";
      forget.textContent = "Remove";
      forget.setAttribute("aria-label", `Remove ${labelFor(entry.template)} from recent jobs`);
      forget.addEventListener("click", () => {
        saveRecent(loadRecent().filter((x) => x.id !== entry.id));
        renderRecent();
      });
      end.append(label, forget);

      item.append(main, end);
      return item;
    }));
  }

  /* ---------- wiring ---------- */

  function onHash() {
    const match = /^#job=(.+)$/.exec(location.hash);
    if (match && ID_PATTERN.test(match[1]) && match[1] !== state.jobId) openJob(match[1]);
  }

  async function copyLink() {
    const button = $("copy-link");
    try {
      await navigator.clipboard.writeText(location.href);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Copy failed";
    }
    setTimeout(() => { button.textContent = "Copy link"; }, 1600);
  }

  async function init() {
    $("text").addEventListener("input", updateForm);
    $("text").addEventListener("keydown", (event) => {
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) submit();
    });
    $("submit").addEventListener("click", submit);
    $("load-example").addEventListener("click", loadExample);
    $("copy-link").addEventListener("click", copyLink);
    $("clear-recent").addEventListener("click", () => {
      saveRecent([]);
      renderRecent();
    });
    window.addEventListener("hashchange", onHash);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden && state.jobId && state.job && !TERMINAL.has(state.job.status)) {
        stopPolling();
        poll(state.jobId, state.pollGeneration);
      }
    });

    updateForm();
    renderRecent();

    try {
      const data = await api("/api/templates");
      state.templates = data.data;
      renderTemplates();
      updateForm();
      renderRecent();
    } catch (error) {
      $("template-options").replaceChildren();
      showNotice("form-error", `Couldn't load the tasks. ${error.message}`, { retry: () => window.location.reload() });
    }

    onHash();
  }

  init();
})();
