document.addEventListener("DOMContentLoaded", () => {
    const $ = (id) => document.getElementById(id);
    const setText = (id, v) => {
        const el = $(id);
        if (el) el.textContent = v;
    };

    setText("m-samples", (OVERALL.samples ?? 0).toLocaleString());
    setText("m-failures", (OVERALL.failures ?? 0).toLocaleString());

    // error_pct is already percent (0..100)
    setText("m-errorpct", (OVERALL.error_pct ?? 0).toFixed(2) + "%");
    setText("m-tps", (OVERALL.tps ?? 0).toFixed(3));
    setText(
        "m-kbps",
        (OVERALL.kbps_recv ?? 0).toFixed(2) +
        " / " +
        (OVERALL.kbps_sent ?? 0).toFixed(2)
    );

    function td(v) {
        const d = document.createElement("td");
        d.textContent =
            typeof v === "number" ? v.toLocaleString() : v ?? "";
        return d;
    }

    const tbodyL = document.querySelector("#tbl-labels tbody");
    if (tbodyL && Array.isArray(LABELS)) {
        LABELS.forEach((L) => {
            const tr = document.createElement("tr");
            tr.append(td(L.label));
            tr.append(td(L.count));
            tr.append(td(L.fails));
            tr.append(td((L.error_pct ?? 0).toFixed(2) + "%"));
            tr.append(td((L.avg_ms ?? 0).toFixed(2)));
            tr.append(td(L.min_ms ?? 0));
            tr.append(td(L.max_ms ?? 0));
            tr.append(td(L.p50_ms ?? 0));
            tr.append(td(L.p90_ms ?? 0));
            tr.append(td(L.p95_ms ?? 0));
            tr.append(td(L.p99_ms ?? 0));
            tr.append(td((L.tps ?? 0).toFixed(3)));
            tr.append(td((L.kbps_recv ?? 0).toFixed(3)));
            tr.append(td((L.kbps_sent ?? 0).toFixed(3)));
            tbodyL.append(tr);
        });
    }

    const totalErrors = (ERRORS || []).reduce((a, e) => a + (e.count || 0), 0);
    const errRate = (OVERALL.error_pct ?? 0).toFixed(2);
    const summary = `Total errors: ${totalErrors.toLocaleString()}  •  Error rate: ${errRate}%  •  Samples: ${(OVERALL.samples || 0).toLocaleString()}`;
    const sumEl = $("error-summary");
    if (sumEl) sumEl.textContent = summary;

    const tbodyE = document.querySelector("#tbl-errors tbody");
    if (tbodyE && Array.isArray(ERRORS)) {
        ERRORS.forEach((E) => {
            const tr = document.createElement("tr");
            tr.append(td(E.response_code));
            tr.append(td(E.response_message));
            tr.append(td(E.count));
            // NOTE: updated field names
            tr.append(td((E.error_pct ?? 0).toFixed(2) + "%"));
            tr.append(td((E.sample_pct ?? 0).toFixed(2) + "%"));
            tbodyE.append(tr);
        });
    }

    function withChartJs(cb, tries = 0) {
        if (window.Chart) return cb();
        if (tries > 200) {
            const note = $("charts-note");
            if (note)
                note.textContent =
                    "Charts unavailable (Chart.js failed to load). If you opened this file offline, internet access is required for charts.";
            return;
        }
        setTimeout(() => withChartJs(cb, tries + 1), 50);
    }

    const clip = (s, n = 80) =>
        s && s.length > n ? s.slice(0, n - 1) + "…" : s ?? "";

    withChartJs(() => {
        const labels = (LABELS || []).map((x) => x.label);
        const countData = (LABELS || []).map((x) => x.count);
        const failData = (LABELS || []).map((x) => x.fails);
        const p95Data = (LABELS || []).map((x) => x.p95_ms);

        const errTop = (ERRORS || []).slice(0, 10);
        const errNames = errTop.map(
            (e) => `${clip(e.response_code, 24)} ${clip(e.response_message, 80)}`
        );
        const errCounts = errTop.map((e) => e.count);

        const elCounts = $("chartCounts");
        const elP95 = $("chartP95");
        const elErrors = $("chartErrors");

        const common = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { labels: { color: "#cfe3ff" } } },
            scales: {
                x: { ticks: { color: "#cfe3ff" } },
                y: { ticks: { color: "#cfe3ff" } },
            },
        };

        if (elCounts)
            new Chart(elCounts, {
                type: "bar",
                data: {
                    labels,
                    datasets: [
                        { label: "Count", data: countData },
                        { label: "Failures", data: failData },
                    ],
                },
                options: common,
            });

        if (elP95)
            new Chart(elP95, {
                type: "bar",
                data: { labels, datasets: [{ label: "p95 (ms)", data: p95Data }] },
                options: common,
            });

        if (elErrors)
            new Chart(elErrors, {
                type: "pie",
                data: {
                    labels: errNames,
                    datasets: [{ label: "Errors (top 10)", data: errCounts }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: "right", labels: { color: "#cfe3ff" } },
                    },
                },
            });
    });
});
