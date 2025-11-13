/**
 * trends.js
 * - Unifica PM2.5, PM10, O3 y NO2 en una sola gráfica con leyenda clicable.
 * - Hace la torta (doughnut) de AQI más pequeña y responsive.
 * - Se refresca cuando cambias de marcador (showPointDetails) y cuando presionas "Generar Interpretación".
 * - Obtiene datos desde /api/historical para garantizar O3 y NO2.
 */

(function () {
  const API_BASE = "http://127.0.0.1:5000/api";
  let currentLocationId = "aguachica_general";

  let combinedChart = null;
  let aqiChart = null;

  const el = (id) => document.getElementById(id);

  function formatHour(ts) {
    try {
      const d = new Date(ts);
      return d.toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' });
    } catch {
      return ts;
    }
  }

  function mergeLabels(seriesList) {
    const set = new Set();
    seriesList.forEach(s => s.forEach(p => set.add(p.t)));
    const arr = Array.from(set);
    arr.sort((a,b) => new Date(a) - new Date(b));
    return arr;
  }

  function mapSeriesToLabels(series, labels) {
    const map = new Map(series.map(p => [p.t, p.v]));
    return labels.map(l => map.has(l) ? map.get(l) : null);
  }

  function buildCombinedChart(pm25, pm10, o3, no2) {
    const ctx = el("combinedTrend").getContext("2d");
    const labels = mergeLabels([pm25, pm10, o3, no2]);
    const labelStrs = labels.map(formatHour);

    const data = {
      labels: labelStrs,
      datasets: [
        {
          label: "PM2.5",
          data: mapSeriesToLabels(pm25, labels),
          borderColor: "#ef4444",
          backgroundColor: "rgba(239,68,68,.15)",
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2,
          spanGaps: true,
        },
        {
          label: "PM10",
          data: mapSeriesToLabels(pm10, labels),
          borderColor: "#f59e0b",
          backgroundColor: "rgba(245,158,11,.15)",
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2,
          spanGaps: true,
        },
        {
          label: "O3",
          data: mapSeriesToLabels(o3, labels),
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59,130,246,.12)",
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2,
          spanGaps: true,
        },
        {
          label: "NO2",
          data: mapSeriesToLabels(no2, labels),
          borderColor: "#10b981",
          backgroundColor: "rgba(16,185,129,.12)",
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 2,
          spanGaps: true,
        },
      ],
    };

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          display: true,
          position: "top",
          labels: { usePointStyle: true },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const v = ctx.parsed.y;
              return `${ctx.dataset.label}: ${v == null ? "—" : v.toFixed(2)} µg/m³`;
            },
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "Hora" } },
        y: { title: { display: true, text: "µg/m³" }, beginAtZero: true },
      },
    };

    if (combinedChart) combinedChart.destroy();
    combinedChart = new Chart(ctx, { type: "line", data, options });
  }

  function buildAQIDonut(dist) {
    const ctx = el("aqiDistribution").getContext("2d");
    const labels = ["1 (Bueno)", "2 (Aceptable)", "3 (Moderado)", "4 (Deficiente)", "5 (Muy deficiente)"];
    const data = labels.map((_, i) => dist[String(i + 1)] || 0);

    if (aqiChart) aqiChart.destroy();
    aqiChart = new Chart(ctx, {
      type: "doughnut",
      data: {
        labels,
        datasets: [{
          data,
          backgroundColor: ["#22c55e", "#84cc16", "#f59e0b", "#f97316", "#ef4444"],
          borderWidth: 1,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: "bottom" },
        },
        cutout: "58%",
      },
    });
  }

  async function fetchHistorical(locationId, days=1, limit=1000) {
    const url = `${API_BASE}/historical/${encodeURIComponent(locationId)}?days=${days}&limit=${limit}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error("HTTP " + r.status);
    const json = await r.json();
    if (!json.success) throw new Error(json.error || "error");
    return json.data || [];
  }

  function seriesFromRows(rows, key) {
    // rows: [{timestamp, pm2_5, pm10, o3, no2, aqi, ...}]
    const out = [];
    // Orden ascendente por timestamp
    rows.sort((a,b) => new Date(a.timestamp) - new Date(b.timestamp));
    for (const row of rows) {
      const v = row[key];
      if (v !== null && v !== undefined) {
        out.push({ t: row.timestamp, v: Number(v) });
      }
    }
    return out;
  }

  async function updateTrendsCharts(locationId) {
    currentLocationId = locationId || currentLocationId;
    const trendsSection = el("trendsSection");
    const msg = el("trendsMsg");
    msg.textContent = "Generando tendencias...";

    try {
      // 24h para series
      const rows24h = await fetchHistorical(currentLocationId, 1, 2000);
      const pm25 = seriesFromRows(rows24h, "pm2_5");
      const pm10 = seriesFromRows(rows24h, "pm10");
      const o3   = seriesFromRows(rows24h, "o3");
      const no2  = seriesFromRows(rows24h, "no2");

      buildCombinedChart(pm25, pm10, o3, no2);

      // 7 días para distribución AQI
      const rows7d = await fetchHistorical(currentLocationId, 7, 10000);
      const dist = { "1":0, "2":0, "3":0, "4":0, "5":0 };
      for (const r of rows7d) {
        const k = String(Math.round(r.aqi || 0));
        if (dist[k] !== undefined) dist[k] += 1;
      }
      buildAQIDonut(dist);

      if (pm25.length + pm10.length + o3.length + no2.length === 0) {
        msg.textContent = "No se pudieron generar las tendencias. Revisa que haya datos históricos suficientes.";
      } else {
        msg.textContent = "Tendencias de las últimas 24 horas y distribución de AQI (7 días).";
      }
      trendsSection.hidden = false;
    } catch (e) {
      msg.textContent = "Error consultando el servidor para generar tendencias.";
      trendsSection.hidden = false;
      console.error(e);
    }
  }

  // Botón "Generar Interpretación" también mostrará/actualizará tendencias
  document.addEventListener("DOMContentLoaded", () => {
    const btn = document.getElementById("btnGenerarInterpretacion");
    if (btn) {
      btn.addEventListener("click", () => updateTrendsCharts(currentLocationId));
    }
    // Cargar una vez por defecto en la vista inicial
    setTimeout(() => updateTrendsCharts(currentLocationId), 300);
  });

  // Exponer función global para que index.html la invoque al cambiar de marcador
  window.updateTrendsCharts = updateTrendsCharts;
})();
