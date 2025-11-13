
/**
 * trends.js (versión promedios de TODOS los puntos)
 * - Calcula las tendencias de las últimas 24 h (de 05:00 a 04:59 del día siguiente)
 *   como el PROMEDIO de PM2.5, PM10, O3 y NO2 a través de TODOS los puntos de interés.
 * - Muestra un gráfico combinado (4 líneas) y un donut de distribución de AQI de 7 días.
 * - Se ejecuta solo cuando el usuario hace clic en el botón "Generar Interpretación".
 *
 * Requisitos:
 *  - API disponible en /api/locations y /api/historical/:id
 *  - Chart.js 4.x cargado en index.html
 */

(function () {
  // ====== Config ======
  const TZ = "America/Bogota";
  const API_BASE = (window.API_BASE || "http://127.0.0.1:5000").replace(/\/$/, "");
  const trendsSection = document.getElementById("trendsSection");
  const trendsMsg = document.getElementById("trendsMsg");
  const btn = document.getElementById("btnGenerarInterpretacion");
  const combinedTrendCanvas = document.getElementById("combinedTrend");
  const aqiDistributionCanvas = document.getElementById("aqiDistribution");

  // Colores distintivos para comparar 4 contaminantes
  const COLORS = {
    pm25: "rgba(244, 63, 94, 1)",   // rojo
    pm10: "rgba(59, 130, 246, 1)",  // azul
    o3:   "rgba(34, 197, 94, 1)",   // verde
    no2:  "rgba(234, 179, 8, 1)"    // amarillo
  };

  let combinedChart = null;
  let aqiDonut = null;

  // Exponer función por compatibilidad con index.html (se ignora pointId: siempre promedio global)
  window.updateTrendsCharts = async function updateTrendsCharts(/* pointId */) {
    await renderAll();
  };

  // Ejecutar al presionar el botón
  if (btn) {
    btn.addEventListener("click", async () => {
      await renderAll();
      if (trendsSection?.hasAttribute("hidden")) {
        trendsSection.removeAttribute("hidden");
      }
      // Scroll suave hacia la sección
      trendsSection?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  // ====== Helpers de tiempo ======
  function toBogota(dateLike) {
    // Convierte un ISO UTC a objeto Date, luego ajusta a hora local del navegador.
    // Para agrupar por hora en Bogotá, usaremos las funciones de Intl.DateTimeFormat.
    const d = new Date(dateLike);
    return d; // usaremos formateadores con TZ para obtener la hora en Bogotá
  }

  /**
   * Devuelve ventana de 24 h [start, end) con inicio 05:00 del "día de ayer" (zona Bogotá)
   * y fin 04:59:59 del "día de hoy".
   */
  function getWindow24h_5to4() {
    const now = new Date();
    // Construimos "hoy" en Bogotá
    const fmtDate = new Intl.DateTimeFormat("es-CO", {
      timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit"
    });
    const parts = fmtDate.formatToParts(now);
    const y = parts.find(p => p.type === "year").value;
    const m = parts.find(p => p.type === "month").value;
    const d = parts.find(p => p.type === "day").value;

    // Crear 04:59:59 de HOY (fin de ventana) en Bogotá
    const endLocal = new Date(`${y}-${m}-${d}T04:59:59`);
    // Para evitar desfases por TZ, obtenemos timestamp aplicando offset de Bogotá
    const endZoned = zonedTimeToUtc(endLocal, TZ);

    // Inicio es el día anterior a las 05:00:00 Bogotá
    const startLocal = new Date(endLocal);
    startLocal.setDate(startLocal.getDate() - 1);
    startLocal.setHours(5, 0, 0, 0);
    const startZoned = zonedTimeToUtc(startLocal, TZ);

    return { start: startZoned, end: endZoned };
  }

  // Polyfill mínimo: convertir fecha "local en TZ" a UTC (sin librerías externas)
  function zonedTimeToUtc(localDate, timeZone) {
    // Interpreta localDate como si estuviera en "timeZone", y devuelve Date en UTC del mismo instante
    const fmt = new Intl.DateTimeFormat("en-US", {
      timeZone, hour12: false,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit"
    });
    const parts = fmt.formatToParts(localDate);
    const y = Number(parts.find(p => p.type === "year").value);
    const m = Number(parts.find(p => p.type === "month").value);
    const d = Number(parts.find(p => p.type === "day").value);
    const hh = Number(parts.find(p => p.type === "hour").value);
    const mm = Number(parts.find(p => p.type === "minute").value);
    const ss = Number(parts.find(p => p.type === "second").value);
    // Construir fecha como si fuera UTC
    return new Date(Date.UTC(y, m - 1, d, hh, mm, ss));
  }

  function formatHourLabel(idxStartAt5) {
    // Genera etiquetas 24h iniciando en 05:00 → 04:00
    const labels = [];
    for (let i = 0; i < 24; i++) {
      const hour = (5 + i) % 24;
      labels.push(String(hour).padStart(2, "0") + ":00");
    }
    return labels;
  }

  // ====== Fetch helpers ======
  async function fetchJson(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const j = await r.json();
    if (!j.success) throw new Error(j.error || "Error de API");
    return j.data;
  }

  async function getAllLocations() {
    return await fetchJson(`${API_BASE}/api/locations`);
  }

  async function getHistoryForLocation(id, days = 2, limit = 2000) {
    return await fetchJson(`${API_BASE}/api/historical/${encodeURIComponent(id)}?days=${days}&limit=${limit}`);
  }

  // ====== Agregación: promedio horario sobre TODOS los puntos ======
  function aggregateHourlyAverages(allRecords, windowStart, windowEnd) {
    // Estructuras de suma y conteo por hora (24 bins 05→04)
    const bins = Array.from({ length: 24 }, () => ({
      sum_pm25: 0, c_pm25: 0,
      sum_pm10: 0, c_pm10: 0,
      sum_o3:   0, c_o3:   0,
      sum_no2:  0, c_no2:  0
    }));

    // Para localizar la hora 05:00 como índice 0
    function hourToIndex(h) {
      // h = 0..23 (hora Bogotá)
      const idx = (h - 5);
      return (idx < 0) ? (24 + idx) : idx; // rotado
    }

    const hourFmt = new Intl.DateTimeFormat("en-CA", { timeZone: TZ, hour: "2-digit", hour12: false });

    for (const rec of allRecords) {
      const ts = new Date(rec.timestamp); // timestamp ISO UTC guardado en DB
      if (!(ts >= windowStart && ts <= windowEnd)) continue;

      // Obtener hora en Bogotá (00..23)
      const parts = hourFmt.formatToParts(ts);
      const h = Number(parts.find(p => p.type === "hour").value);
      const idx = hourToIndex(h);

      const pm25 = safeNumber(rec.pm2_5);
      const pm10 = safeNumber(rec.pm10);
      const o3   = safeNumber(rec.o3);
      const no2  = safeNumber(rec.no2);

      if (pm25 != null) { bins[idx].sum_pm25 += pm25; bins[idx].c_pm25++; }
      if (pm10 != null) { bins[idx].sum_pm10 += pm10; bins[idx].c_pm10++; }
      if (o3   != null) { bins[idx].sum_o3   += o3;   bins[idx].c_o3++;   }
      if (no2  != null) { bins[idx].sum_no2  += no2;  bins[idx].c_no2++;  }
    }

    // Calcular promedios
    const avg_pm25 = bins.map(b => (b.c_pm25 ? +(b.sum_pm25 / b.c_pm25).toFixed(2) : null));
    const avg_pm10 = bins.map(b => (b.c_pm10 ? +(b.sum_pm10 / b.c_pm10).toFixed(2) : null));
    const avg_o3   = bins.map(b => (b.c_o3   ? +(b.sum_o3   / b.c_o3).toFixed(2)   : null));
    const avg_no2  = bins.map(b => (b.c_no2  ? +(b.sum_no2  / b.c_no2).toFixed(2)  : null));

    return { avg_pm25, avg_pm10, avg_o3, avg_no2 };
  }

  function safeNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  // ====== Donut de distribución de AQI (7 días, todos los puntos) ======
  function buildAqiHistogram(allRecords7d) {
    const hist = [0, 0, 0, 0, 0]; // índices 0..4 -> AQI 1..5
    for (const r of allRecords7d) {
      const aqi = Number(r.aqi);
      if (Number.isFinite(aqi) && aqi >= 1 && aqi <= 5) {
        hist[aqi - 1]++;
      }
    }
    return hist;
  }

  // ====== Render principal ======
  async function renderAll() {
    try {
      trendsMsg.textContent = "Calculando promedios de todos los puntos (24 h, 05:00–04:00)...";
      const { start, end } = getWindow24h_5to4();

      // 1) Todas las ubicaciones
      const locs = await getAllLocations();
      if (!locs?.length) {
        throw new Error("No hay ubicaciones disponibles en la base de datos.");
      }

      // 2) Descarga históricos en paralelo (2 días para cubrir la ventana 05→04)
      const histories = await Promise.all(
        locs.map(l => getHistoryForLocation(l.id, 2, 4000).catch(() => []))
      );

      // "histories" es un array de arrays; aplanamos
      const all24h = histories.flat();

      // 3) Filtrar ventana y promediar por hora (todos los puntos)
      const { avg_pm25, avg_pm10, avg_o3, avg_no2 } = aggregateHourlyAverages(all24h, start, end);
      const labels = formatHourLabel();

      // 4) Pintar/actualizar gráfico combinado
      renderCombinedChart(labels, avg_pm25, avg_pm10, avg_o3, avg_no2);

      // 5) Donut AQI últimos 7 días (todos los puntos)
      const histories7d = await Promise.all(
        locs.map(l => getHistoryForLocation(l.id, 7, 10000).catch(() => []))
      );
      const all7d = histories7d.flat();
      renderAqiDonut(buildAqiHistogram(all7d));

      const fmt = new Intl.DateTimeFormat("es-CO", { timeZone: TZ, dateStyle: "medium", timeStyle: "short" });
      trendsMsg.textContent = `Promedio de ${locs.length} puntos. Ventana: ${fmt.format(start)} → ${fmt.format(end)} (${TZ}).`;
    } catch (err) {
      console.error(err);
      trendsMsg.textContent = "No se pudieron generar las tendencias. Revisa que haya datos históricos suficientes.";
    }
  }

  function renderCombinedChart(labels, pm25, pm10, o3, no2) {
    if (!combinedTrendCanvas) return;
    const data = {
      labels,
      datasets: [
        {
          label: "PM₂.₅",
          data: pm25,
          borderColor: COLORS.pm25,
          backgroundColor: COLORS.pm25,
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2
        },
        {
          label: "PM₁₀",
          data: pm10,
          borderColor: COLORS.pm10,
          backgroundColor: COLORS.pm10,
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2
        },
        {
          label: "O₃",
          data: o3,
          borderColor: COLORS.o3,
          backgroundColor: COLORS.o3,
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2
        },
        {
          label: "NO₂",
          data: no2,
          borderColor: COLORS.no2,
          backgroundColor: COLORS.no2,
          tension: 0.25,
          borderWidth: 2,
          pointRadius: 2
        }
      ]
    };

    const options = {
      responsive: true,
      plugins: {
        legend: {
          display: true,
          position: "top"
          // (Chart.js ya permite ocultar/mostrar datasets haciendo clic en la leyenda)
        },
        tooltip: {
          mode: "index",
          intersect: false
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: { title: { display: true, text: "Hora (Bogotá)" } },
        y: { title: { display: true, text: "Concentración (µg/m³)" }, beginAtZero: true }
      }
    };

    if (combinedChart) {
      combinedChart.data = data;
      combinedChart.options = options;
      combinedChart.update();
    } else {
      combinedChart = new Chart(combinedTrendCanvas.getContext("2d"), {
        type: "line",
        data,
        options
      });
    }
  }

  function renderAqiDonut(hist) {
    if (!aqiDistributionCanvas) return;
    const data = {
      labels: ["1 Bueno", "2 Aceptable", "3 Moderado", "4 Deficiente", "5 Muy deficiente"],
      datasets: [
        {
          data: hist,
          backgroundColor: [
            "rgba(34,197,94,0.8)",   // 1 Verde
            "rgba(234,179,8,0.8)",   // 2 Amarillo
            "rgba(59,130,246,0.8)",  // 3 Azul
            "rgba(249,115,22,0.8)",  // 4 Naranja
            "rgba(239,68,68,0.8)"    // 5 Rojo
          ],
          borderWidth: 1
        }
      ]
    };
    const options = {
      responsive: true,
      plugins: {
        legend: { position: "bottom" },
        tooltip: { callbacks: {
          label: (ctx) => {
            const total = hist.reduce((a,b)=>a+b,0) || 1;
            const val = ctx.parsed;
            const pct = ((val*100)/total).toFixed(1) + "%";
            return `${ctx.label}: ${val} (${pct})`;
          }
        } }
      },
      cutout: "65%"
    };

    if (aqiDonut) {
      aqiDonut.data = data;
      aqiDonut.options = options;
      aqiDonut.update();
    } else {
      aqiDonut = new Chart(aqiDistributionCanvas.getContext("2d"), {
        type: "doughnut",
        data,
        options
      });
    }
  }

  // (Opcional) Si quieres que al cargar la página se calcule automáticamente, descomenta:
  // renderAll();
})();
