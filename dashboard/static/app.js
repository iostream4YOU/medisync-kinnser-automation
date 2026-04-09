const state = {
  data: null,
  selectedPatientId: "",
  filters: {
    search: "",
    initial: "ALL",
    documentFilter: "all",
    sortBy: "name",
  },
  autoRefresh: true,
  autoRefreshMs: 45000,
  timerId: null,
};

const refs = {
  connectionBadge: document.getElementById("connectionBadge"),
  generatedAt: document.getElementById("generatedAt"),
  refreshButton: document.getElementById("refreshButton"),
  autoRefreshButton: document.getElementById("autoRefreshButton"),
  searchInput: document.getElementById("searchInput"),
  documentFilter: document.getElementById("documentFilter"),
  sortBy: document.getElementById("sortBy"),
  letterStrip: document.getElementById("letterStrip"),
  filteredSummary: document.getElementById("filteredSummary"),
  patientList: document.getElementById("patientList"),
  patientDetailContent: document.getElementById("patientDetailContent"),
  kpiPatients: document.getElementById("kpiPatients"),
  kpiOrders: document.getElementById("kpiOrders"),
  kpiDocuments: document.getElementById("kpiDocuments"),
  kpiLastRun: document.getElementById("kpiLastRun"),
  cursorGlow: document.getElementById("cursorGlow"),
  pdfModal: document.getElementById("pdfModal"),
  pdfFrame: document.getElementById("pdfFrame"),
  pdfTitle: document.getElementById("pdfTitle"),
  closePdfModal: document.getElementById("closePdfModal"),
};

const PLACEHOLDER_VALUES = new Set([
  "n/a",
  "na",
  "none",
  "null",
  "unknown",
  "not available",
  "not entered",
  "-",
  "--",
  "n\\a",
  "primary insurance secondary insurance tertiary insurance",
  "address phone relationship",
]);

function numberFormat(value) {
  return new Intl.NumberFormat("en-US").format(Number(value || 0));
}

function formatDate(value) {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function cleanValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  const compact = String(value).replace(/\s+/g, " ").trim();
  if (!compact) {
    return "";
  }

  const lowered = compact.toLowerCase();
  if (PLACEHOLDER_VALUES.has(lowered)) {
    return "";
  }
  if (/^n\s*\/\s*a$/i.test(compact)) {
    return "";
  }

  return compact;
}

function firstMeaningful(values) {
  for (const candidate of values) {
    const clean = cleanValue(candidate);
    if (clean) {
      return clean;
    }
  }
  return "";
}

function uniqueMeaningful(values) {
  const seen = new Set();
  const rows = [];

  for (const candidate of values) {
    const clean = cleanValue(candidate);
    if (!clean) {
      continue;
    }
    const key = clean.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    rows.push(clean);
  }

  return rows;
}

function fieldKey(label) {
  return cleanValue(label)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function buildMetaCard(label, value) {
  const clean = cleanValue(value);
  if (!clean) {
    return "";
  }

  return `
    <article class="meta-card">
      <p class="meta-label">${escapeHtml(label)}</p>
      <p class="meta-value">${escapeHtml(clean)}</p>
    </article>
  `;
}

function setLoadingState() {
  refs.patientList.innerHTML = '<div class="empty-state">Loading patient records...</div>';
  refs.patientDetailContent.innerHTML = '<div class="empty-state">Loading patient details...</div>';
}

function setErrorState(message) {
  refs.connectionBadge.textContent = "Disconnected";
  refs.connectionBadge.classList.add("error");
  refs.generatedAt.textContent = "Last update: unavailable";
  refs.patientList.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  refs.patientDetailContent.innerHTML = '<div class="empty-state">Refresh to try again.</div>';
}

function buildQuery(forceRefresh) {
  const params = new URLSearchParams();
  if (state.filters.search) {
    params.set("search", state.filters.search);
  }
  if (state.filters.initial && state.filters.initial !== "ALL") {
    params.set("initial", state.filters.initial);
  }
  params.set("document_filter", state.filters.documentFilter);
  params.set("sort_by", state.filters.sortBy);
  if (forceRefresh) {
    params.set("force_refresh", "true");
  }
  return params.toString();
}

async function fetchDashboard(forceRefresh = false) {
  const query = buildQuery(forceRefresh);
  const endpoint = query ? `/api/dashboard?${query}` : "/api/dashboard";
  const response = await fetch(endpoint, { cache: "no-store" });
  const payload = await response.json();

  if (!response.ok) {
    const message = payload.error || "Unable to load dashboard data";
    throw new Error(message);
  }
  return payload;
}

function renderConnection(payload) {
  const connected = Boolean(payload.connected);
  const badgeText = connected ? "Live" : "Disconnected";
  refs.connectionBadge.textContent = badgeText;
  refs.connectionBadge.classList.toggle("error", !connected);

  const refreshedAt = formatDate(payload.generatedAt);
  refs.generatedAt.textContent = `Last update: ${refreshedAt || "--"}`;
}

function renderSummary(payload) {
  const summary = payload.summary || {};
  const filtered = payload.filteredSummary || {
    visiblePatients: summary.totalPatients || 0,
    visibleOrders: summary.totalOrders || 0,
    visibleDocuments: summary.totalDocuments || 0,
  };

  refs.kpiPatients.textContent = numberFormat(summary.totalPatients);
  refs.kpiOrders.textContent = numberFormat(summary.totalOrders);
  refs.kpiDocuments.textContent = numberFormat(summary.totalDocuments);
  refs.kpiLastRun.textContent = formatDate(summary.lastRunAt) || "No update yet";

  refs.filteredSummary.textContent =
    `${numberFormat(filtered.visiblePatients)} shown, ` +
    `${numberFormat(filtered.visibleOrders)} orders, ` +
    `${numberFormat(filtered.visibleDocuments)} PDFs`;
}

function renderLetterStrip(patients) {
  const counts = {};
  for (const patient of patients) {
    const letter = (patient.initial || patient.name || "").charAt(0).toUpperCase();
    if (!letter || !/[A-Z]/.test(letter)) {
      continue;
    }
    counts[letter] = (counts[letter] || 0) + 1;
  }

  const letters = ["ALL", ..."ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("")];
  refs.letterStrip.innerHTML = letters
    .map((letter) => {
      const isActive = state.filters.initial === letter;
      const countText = letter === "ALL" ? patients.length : counts[letter] || 0;
      return `
        <button
          type="button"
          class="letter-chip ${isActive ? "active" : ""}"
          data-letter="${letter}"
        >${letter} (${countText})</button>
      `;
    })
    .join("");

  refs.letterStrip.querySelectorAll(".letter-chip").forEach((button) => {
    button.addEventListener("click", () => {
      state.filters.initial = button.dataset.letter || "ALL";
      loadDashboard(false);
    });
  });
}

function ensureSelectedPatient(patients) {
  if (!patients.length) {
    state.selectedPatientId = "";
    return;
  }

  const stillVisible = patients.some((patient) => patient.id === state.selectedPatientId);
  if (!stillVisible) {
    state.selectedPatientId = patients[0].id;
  }
}

function renderPatientList(patients) {
  if (!patients.length) {
    refs.patientList.innerHTML = '<div class="empty-state">No patients match your filters.</div>';
    return;
  }

  refs.patientList.innerHTML = patients
    .map((patient, index) => {
      const selectedClass = patient.id === state.selectedPatientId ? "selected" : "";
      const patientName = cleanValue(patient.name) || "Patient";
      const patientMrn = cleanValue(patient.mrn);
      const mrnMarkup = patientMrn ? `<p class="patient-mrn">MRN: ${escapeHtml(patientMrn)}</p>` : "";
      return `
        <article class="patient-row ${selectedClass}" data-patient-id="${escapeHtml(patient.id)}" style="animation-delay:${
        Math.min(index * 35, 360)
      }ms">
          <div class="patient-title">
            <p class="patient-name">${escapeHtml(patientName)}</p>
            ${mrnMarkup}
          </div>
          <div class="patient-metrics">
            <span class="metric-badge">Orders: ${numberFormat(patient.orderCount)}</span>
            <span class="metric-badge ${patient.documentCount ? "" : "warn"}">Docs: ${numberFormat(
        patient.documentCount
      )}</span>
            <span class="metric-badge">Updated: ${escapeHtml(formatDate(patient.latestUpdate))}</span>
          </div>
        </article>
      `;
    })
    .join("");

  refs.patientList.querySelectorAll(".patient-row").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedPatientId = row.dataset.patientId || "";
      renderPatientList(patients);
      renderPatientDetails(patients);
    });
  });
}

function renderPatientDetails(patients) {
  const patient = patients.find((entry) => entry.id === state.selectedPatientId);
  if (!patient) {
    refs.patientDetailContent.innerHTML = '<div class="empty-state">Select a patient to view profile and orders PDFs.</div>';
    return;
  }

  const orders = patient.orders || [];
  const primaryPhysician = firstMeaningful([patient.primaryPhysicianName, ...orders.map((order) => order.physicianName)]);
  const ssnInput = cleanValue(patient.ssn);
  const ssnRaw = String(ssnInput || "").replace(/\D/g, "");
  const ssnMasked = ssnRaw.length === 9 ? `***-**-${ssnRaw.slice(-4)}` : ssnInput;
  const cityState = [cleanValue(patient.city), cleanValue(patient.state)].filter(Boolean).join(", ");
  const profileTimestamp = firstMeaningful([patient.profileExtractedAt, patient.latestUpdate]);
  const profileTimestampText = profileTimestamp ? formatDate(profileTimestamp) : "";

  const profileLink = patient.profileUrl
    ? `<a class="link-button" href="${escapeHtml(patient.profileUrl)}" target="_blank" rel="noopener">Open Profile</a>`
    : "";

  const profileFieldRows = [
    ["Patient Name", patient.name],
    ["MRN", patient.mrn],
    ["DOB", patient.dob],
    ["Gender", patient.gender],
    ["Episode", patient.episode],
    ["Phone", patient.phone],
    ["Email", patient.email],
    ["SSN", ssnMasked],
    ["Address", patient.address],
    ["City", patient.city],
    ["State", patient.state],
    ["Zip", patient.zip],
    ["Marital Status", patient.maritalStatus],
    ["Primary Language", patient.primaryLanguage],
    ["Insurance", patient.insurance],
    ["Emergency Contact", patient.emergencyContact],
    ["Allergies", patient.allergies],
    ["Primary Physician", primaryPhysician],
    ["Diagnoses", patient.diagnoses],
    ["Diagnosis Codes", patient.diagnosisCodes],
    ["Profile Extracted", profileTimestampText],
  ];

  const renderedLabels = new Set();
  const profileCards = [];
  for (const [label, value] of profileFieldRows) {
    const card = buildMetaCard(label, value);
    if (!card) {
      continue;
    }
    profileCards.push(card);
    renderedLabels.add(fieldKey(label));
  }

  const profilePairsRaw = Array.isArray(patient.profilePairs) ? patient.profilePairs.slice(0, 260) : [];
  for (const pair of profilePairsRaw) {
    if (!pair || typeof pair !== "object") {
      continue;
    }

    const label = cleanValue(pair.label);
    const value = cleanValue(pair.value);
    if (!label || !value) {
      continue;
    }

    const labelKey = fieldKey(label);
    if (renderedLabels.has(labelKey)) {
      continue;
    }

    const card = buildMetaCard(label, value);
    if (!card) {
      continue;
    }

    renderedLabels.add(labelKey);
    profileCards.push(card);
    if (profileCards.length >= 42) {
      break;
    }
  }

  const pdfOrders = orders
    .filter((order) => Boolean(order && (order.hasDatabaseDocument || order.hasLocalDocument) && cleanValue(order.documentViewUrl)))
    .sort((left, right) => {
      const leftName = cleanValue(left.documentName) || cleanValue(left.orderType);
      const rightName = cleanValue(right.documentName) || cleanValue(right.orderType);
      const leftCms = /cms\s*485/i.test(leftName);
      const rightCms = /cms\s*485/i.test(rightName);
      if (leftCms !== rightCms) {
        return leftCms ? -1 : 1;
      }

      const leftDate = new Date(cleanValue(left.orderDate) || cleanValue(left.updatedAt) || cleanValue(left.createdAt));
      const rightDate = new Date(cleanValue(right.orderDate) || cleanValue(right.updatedAt) || cleanValue(right.createdAt));
      const leftTime = Number.isNaN(leftDate.getTime()) ? 0 : leftDate.getTime();
      const rightTime = Number.isNaN(rightDate.getTime()) ? 0 : rightDate.getTime();
      return rightTime - leftTime;
    });

  const pdfMarkup = pdfOrders.length
    ? pdfOrders
        .map((order) => {
          const orderNumber = cleanValue(order.orderNumber);
          const orderType = cleanValue(order.orderType);
          const orderDate = cleanValue(order.orderDate);
          const physicianName = cleanValue(order.physicianName);
          const status = cleanValue(order.status) || "Captured";
          const documentName = cleanValue(order.documentName) || orderType || "Order PDF";
          const isCms485 = /cms\s*485/i.test(documentName);

          const orderTopParts = [];
          if (orderNumber) {
            orderTopParts.push(`Order #${escapeHtml(orderNumber)}`);
          }
          if (orderDate) {
            orderTopParts.push(escapeHtml(orderDate));
          }
          if (orderType && orderType.toLowerCase() !== documentName.toLowerCase()) {
            orderTopParts.push(escapeHtml(orderType));
          }

          const detailParts = [];
          if (physicianName) {
            detailParts.push(`Physician: ${escapeHtml(physicianName)}`);
          }

          const viewButton = `<button type="button" class="link-button" data-view-url="${escapeHtml(
            order.documentViewUrl
          )}" data-title="${escapeHtml(documentName)}">View PDF</button>`;

          const downloadButton = cleanValue(order.documentDownloadUrl)
            ? `<a class="link-button" href="${escapeHtml(order.documentDownloadUrl)}" target="_blank" rel="noopener">Download</a>`
            : "";

          const sourceButton = order.documentLink
            ? `<a class="link-button" href="${escapeHtml(order.documentLink)}" target="_blank" rel="noopener">Open Link</a>`
            : "";

          const statusClass = /error|failed/i.test(status) ? "error" : "";
          const orderTopMeta = orderTopParts.length
            ? `<p class="order-sub">${orderTopParts.join(" | ")}</p>`
            : "";
          const detailMeta = detailParts.length
            ? `<p class="order-sub">${detailParts.join(" | ")}</p>`
            : "";
          const cmsTag = isCms485 ? '<span class="order-tag">CMS 485</span>' : "";

          return `
            <article class="order-card ${isCms485 ? "cms485" : ""}">
              <div class="order-top">
                <div>
                  <p class="order-title">${escapeHtml(documentName)}</p>
                  ${orderTopMeta}
                  ${detailMeta}
                </div>
                <div class="order-badge-stack">
                  ${cmsTag}
                  <span class="order-status ${statusClass}">${escapeHtml(status)}</span>
                </div>
              </div>
              <div class="order-actions">
                ${viewButton}
                ${downloadButton}
                ${sourceButton}
              </div>
            </article>
          `;
        })
        .join("")
    : '<div class="empty-state">No order PDFs are stored in the database for this patient yet. Run the CMS 485 workflow and refresh after sync.</div>';

  const patientName = cleanValue(patient.name) || "Patient";

  refs.patientDetailContent.innerHTML = `
    <section class="patient-overview">
      <div class="section-header-inline">
        <h3>${escapeHtml(patientName)} • Patient Profile</h3>
        <span class="muted">Complete patient snapshot</span>
      </div>
      <div class="overview-meta">
        ${profileCards.join("") || '<div class="empty-state">No profile fields captured for this patient yet.</div>'}
      </div>
      <div class="order-actions">
        ${profileLink}
      </div>
    </section>
    <section class="order-list pdf-section">
      <div class="section-header-inline">
        <h3>Orders PDFs</h3>
        <span class="muted">${numberFormat(pdfOrders.length)} ${pdfOrders.length === 1 ? "PDF" : "PDFs"} available</span>
      </div>
      ${pdfMarkup}
    </section>
  `;

  refs.patientDetailContent.querySelectorAll("[data-view-url]").forEach((button) => {
    button.addEventListener("click", () => {
      openPdfModal(button.dataset.viewUrl || "", button.dataset.title || "Document Preview");
    });
  });
}

function openPdfModal(url, title) {
  if (!url) {
    return;
  }
  refs.pdfFrame.src = url;
  refs.pdfTitle.textContent = title || "Document Preview";
  refs.pdfModal.classList.remove("hidden");
}

function closePdfModal() {
  refs.pdfFrame.src = "";
  refs.pdfModal.classList.add("hidden");
}

function applyPayload(payload) {
  state.data = payload;

  renderConnection(payload);
  renderSummary(payload);

  const patients = payload.patients || [];
  renderLetterStrip(patients);
  ensureSelectedPatient(patients);
  renderPatientList(patients);
  renderPatientDetails(patients);
}

function configureInteractiveFx() {
  if (refs.cursorGlow) {
    let rafId = 0;
    const root = document.documentElement;
    const updatePointer = (x, y) => {
      root.style.setProperty("--pointer-x", `${x}px`);
      root.style.setProperty("--pointer-y", `${y}px`);
      refs.cursorGlow.style.transform = `translate(${x}px, ${y}px)`;
    };

    window.addEventListener("mousemove", (event) => {
      if (rafId) {
        window.cancelAnimationFrame(rafId);
      }
      rafId = window.requestAnimationFrame(() => updatePointer(event.clientX, event.clientY));
      refs.cursorGlow.classList.remove("hidden");
    });

    window.addEventListener("mouseleave", () => {
      refs.cursorGlow.classList.add("hidden");
    });

    if (window.matchMedia && window.matchMedia("(pointer: coarse)").matches) {
      refs.cursorGlow.classList.add("hidden");
    }
  }

  const panels = document.querySelectorAll('[data-fx="tilt"]');
  panels.forEach((panel) => {
    if (panel.dataset.fxBound === "true") {
      return;
    }
    panel.dataset.fxBound = "true";

    panel.addEventListener("mousemove", (event) => {
      const rect = panel.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const halfW = rect.width / 2;
      const halfH = rect.height / 2;
      const rotateX = ((y - halfH) / Math.max(halfH, 1)) * -2.4;
      const rotateY = ((x - halfW) / Math.max(halfW, 1)) * 2.8;

      panel.style.setProperty("--fx-x", `${x}px`);
      panel.style.setProperty("--fx-y", `${y}px`);
      panel.style.setProperty("--fx-glow-opacity", "1");
      panel.style.transform = `perspective(900px) rotateX(${rotateX.toFixed(2)}deg) rotateY(${rotateY.toFixed(
        2
      )}deg) translateY(-2px)`;
    });

    panel.addEventListener("mouseleave", () => {
      panel.style.transform = "";
      panel.style.setProperty("--fx-glow-opacity", "0");
    });
  });
}

async function loadDashboard(forceRefresh = false) {
  try {
    if (!state.data) {
      setLoadingState();
    }
    const payload = await fetchDashboard(forceRefresh);
    applyPayload(payload);
  } catch (error) {
    setErrorState(error.message || "Unable to connect to the dashboard API");
  }
}

function debounce(fn, delayMs) {
  let timeoutId = null;
  return (...args) => {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
    timeoutId = window.setTimeout(() => fn(...args), delayMs);
  };
}

function configureControls() {
  refs.refreshButton.addEventListener("click", () => loadDashboard(true));

  refs.autoRefreshButton.addEventListener("click", () => {
    state.autoRefresh = !state.autoRefresh;
    refs.autoRefreshButton.textContent = `Auto Refresh: ${state.autoRefresh ? "On" : "Off"}`;
    configureAutoRefresh();
  });

  refs.documentFilter.addEventListener("change", () => {
    state.filters.documentFilter = refs.documentFilter.value;
    loadDashboard(false);
  });

  refs.sortBy.addEventListener("change", () => {
    state.filters.sortBy = refs.sortBy.value;
    loadDashboard(false);
  });

  const handleSearch = debounce(() => {
    state.filters.search = refs.searchInput.value.trim();
    loadDashboard(false);
  }, 250);
  refs.searchInput.addEventListener("input", handleSearch);

  refs.closePdfModal.addEventListener("click", closePdfModal);
  refs.pdfModal.addEventListener("click", (event) => {
    if (event.target === refs.pdfModal) {
      closePdfModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closePdfModal();
    }
  });
}

function configureAutoRefresh() {
  if (state.timerId) {
    window.clearInterval(state.timerId);
    state.timerId = null;
  }

  if (!state.autoRefresh) {
    return;
  }

  state.timerId = window.setInterval(() => {
    loadDashboard(true);
  }, state.autoRefreshMs);
}

function init() {
  configureInteractiveFx();
  configureControls();
  configureAutoRefresh();
  loadDashboard(true);
}

init();
