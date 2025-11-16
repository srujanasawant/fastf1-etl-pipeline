// ====================
// SAFE FETCH
// ====================
async function safeFetch(url, opts={}) {
    try {
        const r = await fetch(url, opts);
        if (!r.ok) {
            const txt = await r.text();
            throw new Error(`[${r.status}] ${txt}`);
        }
        return r.json();
    } catch (err) {
        console.error(err);
        return { __error: String(err) };
    }
}

// ====================
// HOME PAGE
// ====================
async function initHome() {
    const api = window.apiBase;

    const list = await safeFetch(`${api}/schemas`);
    if (!Array.isArray(list)) {
        document.getElementById("summary-sources-val").innerText = "n/a";
        return;
    }

    const sources = [...new Set(list.map(s => s.source))];

    document.getElementById("summary-sources-val").innerText = sources.join(", ");
    document.getElementById("summary-schemas-val").innerText = `${list.length}`;
}

// ====================
// SESSIONS PAGE
// ====================
async function initSessions() {
    populateSelect("sel-year", [2025, 2024, 2023, 2022, 2021]);

    document.getElementById("btn-load-session")
        .addEventListener("click", loadSelectedSession);
}

async function loadSelectedSession() {
    const api = window.apiBase;
    const year = document.getElementById("sel-year").value;
    const race = document.getElementById("inp-race").value;
    const session = document.getElementById("sel-session").value;

    const url = `${api}/session/fetch?year=${encodeURIComponent(year)}&race=${encodeURIComponent(race)}&session=${encodeURIComponent(session)}`;

    const data = await safeFetch(url);

    if (data.__error) {
        document.getElementById("session-cards").innerHTML =
            `<div class="small-card">Error: ${data.__error}</div>`;
        return;
    }

    // ============================
    // SUMMARY CARDS
    // ============================
    const md = data.metadata;
    document.getElementById("session-cards").innerHTML = `
        <div class="small-card"><b>Session</b><div>${md.session_name || md.session_type}</div></div>
        <div class="small-card"><b>Event</b><div>${md.event || md.event_name}</div></div>
        <div class="small-card"><b>Date</b><div>${md.session_date}</div></div>
        <div class="small-card"><b>Drivers</b><div>${(data.drivers || []).join(", ")}</div></div>
    `;

    // ============================
    // LAPS TABLE
    // ============================
    document.getElementById("laps-table")
        .innerHTML = buildTableFromObject(data.laps, 50);

    // ============================
    // WEATHER TABLE
    // ============================
    if (data.weather) {
        document.getElementById("weather-table")
            .innerHTML = buildTableFromObject(data.weather, 50);
    } else {
        document.getElementById("weather-table").innerHTML = "<div class='muted'>No weather</div>";
    }
}

// ====================
// RAW DOCS PAGE
// ====================
async function initRaw() {
    document.getElementById("btn-list-raw").addEventListener("click", listRawDocs);
}

async function listRawDocs() {
    const api = window.apiBase;
    const source = document.getElementById("raw-source").value;
    const schemaid = document.getElementById("raw-schemaid").value;

    const url = `${api}/raw?source=${encodeURIComponent(source)}${schemaid ? `&schema_id=${encodeURIComponent(schemaid)}` : ""}`;

    const docs = await safeFetch(url);

    const el = document.getElementById("raw-list");
    if (docs.__error) {
        el.innerHTML = docs.__error;
        return;
    }
    if (!docs.length) {
        el.innerHTML = "<div class='muted'>No docs</div>";
        return;
    }

    el.innerHTML = "";
    docs.forEach(d => {
        const div = document.createElement("div");
        div.className = "small-card";
        div.innerHTML = `<b>${d.source}</b><div>${d.schema_id || ""} • ${d.ingested_at}</div>`;
        div.onclick = () => {
            document.getElementById("raw-doc").innerText = JSON.stringify(d, null, 2);
        };
        el.appendChild(div);
    });
}

// ====================
// DIFF PAGE
// ====================
async function initDrift() {
    const api = window.apiBase;

    const all = await safeFetch(`${api}/schemas`);
    const sources = [...new Set(all.map(s => s.source))];

    populateSelect("sel-source", sources);
    await populateVersionsFromSource();

    document.getElementById("btn-diff").addEventListener("click", doCompareVersions);
    document.getElementById("sel-source").addEventListener("change", populateVersionsFromSource);
}

async function populateVersionsFromSource() {
    const api = window.apiBase;
    const source = document.getElementById("sel-source").value;

    const list = await safeFetch(`${api}/schemas?source=${encodeURIComponent(source)}`);
    const versions = list.map(s => s.version).sort((a, b) => a - b);

    populateSelect("sel-v1", versions);
    populateSelect("sel-v2", versions);
}

async function doCompareVersions() {
    const api = window.apiBase;
    const source = document.getElementById("sel-source").value;
    const v1 = document.getElementById("sel-v1").value;
    const v2 = document.getElementById("sel-v2").value;

    const data = await safeFetch(`${api}/schema/diff?source=${source}&v1=${v1}&v2=${v2}`);

    if (data.__error || data.error) {
        document.getElementById("diff-summary").innerText = "Error loading diff";
        return;
    }

    const dd = data.diff;
    renderCompactDiff(dd);

    document.getElementById("raw-diff").innerText = JSON.stringify(dd, null, 2);
}

// ===============================
// TABLE BUILDER (FIXED)
// ===============================
function buildTableFromObject(obj, limit=40) {
    if (!obj) return "<div class='muted'>Empty</div>";

    try {
        const cols = Object.keys(obj);
        const firstCol = obj[cols[0]];

        if (Array.isArray(firstCol)) {
            // Treat as column-oriented dataframe
            let html = "<table class='mini-table'><thead><tr>";
            cols.forEach(c => html += `<th>${c}</th>`);
            html += "</tr></thead><tbody>";

            const rows = firstCol.length;
            const rowLimit = Math.min(rows, limit);

            for (let i = 0; i < rowLimit; i++) {
                html += "<tr>";
                cols.forEach(c => html += `<td>${shortValue(obj[c][i])}</td>`);
                html += "</tr>";
            }
            html += "</tbody></table>";
            return html;
        }

        // Fallback: object of objects or mixed
        let html = "";
        for (let k of Object.keys(obj).slice(0, limit)) {
            html += `<div><b>${k}</b>: ${shortValue(obj[k])}</div>`;
        }
        return html;

    } catch (e) {
        console.warn("[table render error]", e);
        return "<div class='muted'>Cannot render</div>";
    }
}

function shortValue(v) {
    if (v === null || v === undefined) return "<i>null</i>";
    if (typeof v === "object") return "{…}";
    const s = String(v);
    return s.length > 40 ? s.slice(0, 37) + "…" : s;
}

function populateSelect(id, arr) {
    const el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = "";
    arr.forEach(v => {
        const o = document.createElement("option");
        o.value = v;
        o.textContent = v;
        el.appendChild(o);
    });
}
