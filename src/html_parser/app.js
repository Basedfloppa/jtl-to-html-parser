document.addEventListener("DOMContentLoaded", () => {
    const $ = (id) => document.getElementById(id);

    // -------- formatting helpers --------
    const nfInt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
    const nf2 = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const nf3 = new Intl.NumberFormat(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 });

    const fmtInt = (v) => nfInt.format(Number(v || 0));
    const fmt2 = (v) => nf2.format(Number(v || 0));
    const fmt3 = (v) => nf3.format(Number(v || 0));
    const pct2 = (v) => `${fmt2(v)}%`;

    const clip = (s, n = 80) => (s && s.length > n ? s.slice(0, n - 1) + "…" : (s ?? ""));

    // -------- KPI population --------
    const samples = OVERALL?.samples ?? 0;
    const failures = OVERALL?.failures ?? 0;
    const errPct = OVERALL?.error_pct ?? 0;
    const tps = OVERALL?.tps ?? 0;
    const kbpsRecv = OVERALL?.kbps_recv ?? 0;
    const kbpsSent = OVERALL?.kbps_sent ?? 0;

    if ($("m-samples")) $("m-samples").textContent = fmtInt(samples);
    if ($("m-failures")) $("m-failures").textContent = fmtInt(failures);
    if ($("m-tps")) $("m-tps").textContent = fmt3(tps);
    if ($("m-kbps")) $("m-kbps").textContent = `${fmt2(kbpsRecv)} / ${fmt2(kbpsSent)}`;

    const badge = $("m-errorpct");
    if (badge) {
        badge.textContent = pct2(errPct);
        badge.classList.remove("good", "warn", "bad");
        if (errPct < 1) badge.classList.add("good");
        else if (errPct < 5) badge.classList.add("warn");
        else badge.classList.add("bad");
    }

    const statusEl = $("m-status");
    if (statusEl) {
        const status =
            errPct < 1 ? "Status: PASS (error < 1%)" :
                errPct < 5 ? "Status: WARN (error 1–5%)" :
                    "Status: FAIL (error ≥ 5%)";
        statusEl.textContent = status;
    }

    // -------- table rendering --------
    const labelsRaw = Array.isArray(LABELS) ? LABELS.slice() : [];
    const errorsRaw = Array.isArray(ERRORS) ? ERRORS.slice() : [];

    const tableState = {
        labels: { key: "count", dir: "desc", filter: "" },
        errors: { key: "count", dir: "desc" },
    };

    function makeTd(value, isNum = false) {
        const td = document.createElement("td");
        td.textContent = value;
        if (isNum) td.classList.add("num");
        return td;
    }

    function sortBy(arr, key, dir) {
        const m = dir === "asc" ? 1 : -1;
        return arr.slice().sort((a, b) => {
            const av = a?.[key];
            const bv = b?.[key];
            const aNum = typeof av === "number";
            const bNum = typeof bv === "number";
            if (aNum && bNum) return (av - bv) * m;
            return String(av ?? "").localeCompare(String(bv ?? "")) * m;
        });
    }

    function setSortIndicators(tableId, state) {
        const thead = document.querySelector(`#${tableId} thead`);
        if (!thead) return;
        thead.querySelectorAll("th[data-key]").forEach((th) => {
            const key = th.getAttribute("data-key");
            const base = th.textContent.replace(/[\s▲▼]+$/g, "");
            if (key === state.key) th.textContent = `${base} ${state.dir === "asc" ? "▲" : "▼"}`;
            else th.textContent = base;
        });
    }

    function renderLabelsTable() {
        const tbody = document.querySelector("#tbl-labels tbody");
        if (!tbody) return;
        tbody.innerHTML = "";

        const filter = (tableState.labels.filter || "").trim().toLowerCase();

        let data = labelsRaw;
        if (filter) data = data.filter((l) => String(l.label ?? "").toLowerCase().includes(filter));

        data = sortBy(data, tableState.labels.key, tableState.labels.dir);

        data.forEach((L) => {
            const tr = document.createElement("tr");
            tr.append(makeTd(String(L.label ?? "")));

            tr.append(makeTd(fmtInt(L.count ?? 0), true));
            tr.append(makeTd(fmtInt(L.fails ?? 0), true));
            tr.append(makeTd(pct2(L.error_pct ?? 0), true));

            tr.append(makeTd(fmt2(L.avg_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.min_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.max_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.p50_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.p90_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.p95_ms ?? 0), true));
            tr.append(makeTd(fmtInt(L.p99_ms ?? 0), true));

            tr.append(makeTd(fmt3(L.tps ?? 0), true));
            tr.append(makeTd(fmt3(L.kbps_recv ?? 0), true));
            tr.append(makeTd(fmt3(L.kbps_sent ?? 0), true));

            tbody.append(tr);
        });

        setSortIndicators("tbl-labels", tableState.labels);
    }

    function renderErrorsTable() {
        const tbody = document.querySelector("#tbl-errors tbody");
        if (!tbody) return;
        tbody.innerHTML = "";

        const data = sortBy(errorsRaw, tableState.errors.key, tableState.errors.dir);

        data.forEach((E) => {
            const tr = document.createElement("tr");
            tr.append(makeTd(String(E.response_code ?? "")));
            tr.append(makeTd(String(E.response_message ?? "")));

            tr.append(makeTd(fmtInt(E.count ?? 0), true));
            tr.append(makeTd(pct2(E.error_pct ?? 0), true));
            tr.append(makeTd(pct2(E.sample_pct ?? 0), true));

            tbody.append(tr);
        });

        setSortIndicators("tbl-errors", tableState.errors);
    }

    function attachSortHandlers(tableId, stateKey) {
        const table = document.getElementById(tableId);
        if (!table) return;

        table.querySelectorAll("thead th[data-key]").forEach((th) => {
            th.addEventListener("click", () => {
                const key = th.getAttribute("data-key");
                const state = tableState[stateKey];
                if (state.key === key) state.dir = state.dir === "asc" ? "desc" : "asc";
                else { state.key = key; state.dir = "desc"; }

                if (stateKey === "labels") renderLabelsTable();
                else renderErrorsTable();
            });
        });
    }

    attachSortHandlers("tbl-labels", "labels");
    attachSortHandlers("tbl-errors", "errors");

    const filterInput = $("ctl-filter-labels");
    if (filterInput) {
        filterInput.addEventListener("input", () => {
            tableState.labels.filter = filterInput.value || "";
            renderLabelsTable();
        });
    }

    renderLabelsTable();
    renderErrorsTable();

    const totalErrors = errorsRaw.reduce((a, e) => a + (e.count || 0), 0);
    const summary = `Total errors: ${fmtInt(totalErrors)}  •  Error rate: ${pct2(errPct)}  •  Samples: ${fmtInt(samples)}`;
    if ($("error-summary")) $("error-summary").textContent = summary;

    // -------- charts --------
    let chartCounts = null;
    let chartP95 = null;
    let chartErrors = null;

    const note = $("charts-note");

    function withChartJs(cb, tries = 0) {
        if (window.Chart) return cb();
        if (tries > 120) {
            if (note) {
                note.textContent =
                    "Charts unavailable (Chart.js failed to load). If you opened this file offline, internet access is required unless Chart.js is bundled.";
            }
            return;
        }
        setTimeout(() => withChartJs(cb, tries + 1), 50);
    }

    function getTopN(arr, n) {
        if (!n || n <= 0) return arr;
        return arr.slice(0, n);
    }

    function buildCountsData(topN) {
        const data = sortBy(labelsRaw, "count", "desc");
        const top = getTopN(data, topN);

        return {
            labels: top.map((x) => clip(String(x.label ?? ""), 60)),
            countData: top.map((x) => x.count ?? 0),
            failData: top.map((x) => x.fails ?? 0),
        };
    }

    function buildP95Data(topN) {
        const data = sortBy(labelsRaw, "p95_ms", "desc");
        const top = getTopN(data, topN);

        return {
            labels: top.map((x) => clip(String(x.label ?? ""), 60)),
            p95Data: top.map((x) => x.p95_ms ?? 0),
        };
    }

    function destroyCharts() {
        [chartCounts, chartP95, chartErrors].forEach((c) => c && c.destroy());
        chartCounts = chartP95 = chartErrors = null;
    }

    function renderCharts() {
        const elCounts = $("chartCounts");
        const elP95 = $("chartP95");
        const elErrors = $("chartErrors");
        if (!elCounts && !elP95 && !elErrors) return;

        // read controls
        const topn1 = Number($("ctl-topn")?.value ?? 10);
        const topn2 = Number($("ctl-topn2")?.value ?? 10);

        const counts = buildCountsData(topn1);
        const p95 = buildP95Data(topn2);

        const errTop = getTopN(sortBy(errorsRaw, "count", "desc"), 10);
        const errNames = errTop.map((e) => `${clip(e.response_code, 24)} ${clip(e.response_message, 80)}`.trim());
        const errCounts = errTop.map((e) => e.count ?? 0);

        destroyCharts();

        const common = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { labels: { color: "#cfe3ff" } } },
            scales: {
                x: { ticks: { color: "#cfe3ff" }, grid: { color: "rgba(255,255,255,.08)" } },
                y: { ticks: { color: "#cfe3ff" }, grid: { color: "rgba(255,255,255,.08)" } },
            },
        };

        if (elCounts) {
            chartCounts = new Chart(elCounts, {
                type: "bar",
                data: {
                    labels: counts.labels,
                    datasets: [
                        { label: "Count", data: counts.countData },
                        { label: "Failures", data: counts.failData },
                    ],
                },
                options: { ...common, indexAxis: "y" },
            });
        }

        if (elP95) {
            chartP95 = new Chart(elP95, {
                type: "bar",
                data: { labels: p95.labels, datasets: [{ label: "p95 (ms)", data: p95.p95Data }] },
                options: { ...common, indexAxis: "y" },
            });
        }

        if (elErrors) {
            chartErrors = new Chart(elErrors, {
                type: "pie",
                data: { labels: errNames, datasets: [{ label: "Errors (top 10)", data: errCounts }] },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: "right", labels: { color: "#cfe3ff" } },
                    },
                },
            });
        }

        if (note) note.textContent = "";
    }

    const topnCtl = $("ctl-topn");
    const topnCtl2 = $("ctl-topn2");
    if (topnCtl) topnCtl.addEventListener("change", () => withChartJs(renderCharts));
    if (topnCtl2) topnCtl2.addEventListener("change", () => withChartJs(renderCharts));

    withChartJs(renderCharts);
});
