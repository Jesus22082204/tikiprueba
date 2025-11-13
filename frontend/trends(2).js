// trends.js: Genera tendencias de AYER (00:00–23:59, horario de Bogotá) solo al presionar "Generar Interpretación".
// Requiere Chart.js y endpoint /api/trends/<location_id>?day=yesterday

(function(){
  let combinedChart = null;
  let aqiChart = null;
  const COLORS = {
    pm25: 'rgba(220,38,38,0.9)',
    pm10: 'rgba(234,179,8,0.9)',
    o3:   'rgba(59,130,246,0.9)',
    no2:  'rgba(16,185,129,0.9)'
  };

  function fmtHour(tsISO){
    const d = new Date(tsISO);
    const hh = String(d.getHours()).padStart(2,'0');
    const mm = String(d.getMinutes()).padStart(2,'0');
    return `${hh}:${mm}`;
  }

  function buildSeries(data){
    // Unir por timestamp (ordenados en el backend), convertir a arrays
    const lbls = Array.from(new Set(
      [...data.pm25_24h, ...data.pm10_24h, ...data.o3_24h, ...data.no2_24h].map(p=>p.t)
    )).sort((a,b)=> new Date(a) - new Date(b));
    const make = (arr)=> lbls.map(t => {
      const found = arr.find(x=>x.t===t);
      return found ? found.v : null;
    });
    return {
      labels: lbls.map(fmtHour),
      pm25: make(data.pm25_24h),
      pm10: make(data.pm10_24h),
      o3:   make(data.o3_24h),
      no2:  make(data.no2_24h)
    };
  }

  function renderCombined(ctx, s){
    if(combinedChart) combinedChart.destroy();
    combinedChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: s.labels,
        datasets: [
          {label:'PM₂.₅', data:s.pm25, borderColor:COLORS.pm25, backgroundColor:COLORS.pm25, pointRadius:2, tension:.3},
          {label:'PM₁₀', data:s.pm10, borderColor:COLORS.pm10, backgroundColor:COLORS.pm10, pointRadius:2, tension:.3},
          {label:'O₃',   data:s.o3,   borderColor:COLORS.o3,   backgroundColor:COLORS.o3,   pointRadius:2, tension:.3},
          {label:'NO₂',  data:s.no2,  borderColor:COLORS.no2,  backgroundColor:COLORS.no2,  pointRadius:2, tension:.3},
        ]
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        interaction:{mode:'nearest', intersect:false},
        plugins:{ legend:{position:'top'} },
        scales:{ x:{ title:{display:true,text:'Hora'} }, y:{ title:{display:true,text:'µg/m³'}, beginAtZero:true } }
      }
    });
  }

  function renderAQI(ctx, dist){
    if(aqiChart) aqiChart.destroy();
    const labels = ['1 Bueno','2 Aceptable','3 Moderado','4 Deficiente','5 Muy deficiente'];
    const values = [dist['1']||0,dist['2']||0,dist['3']||0,dist['4']||0,dist['5']||0];
    aqiChart = new Chart(ctx, {
      type:'doughnut',
      data:{ labels, datasets:[{ data:values, backgroundColor:['#22c55e','#a3e635','#f59e0b','#f97316','#ef4444'] }] },
      options:{ responsive:true, maintainAspectRatio:false, cutout:'60%', plugins:{legend:{position:'bottom'}} }
    });
  }

  async function generar(){
    const section = document.getElementById('trendsSection');
    const msg = document.getElementById('trendsMsg');
    section.hidden = false; // mostrar solo al presionar
    msg.textContent = 'Generando tendencias de AYER…';

    const loc = window.currentPointId || 'aguachica_general';
    const base = window.API_BASE || 'http://127.0.0.1:5000';

    try{
      const res = await fetch(`${base}/api/trends/${loc}?day=yesterday`);
      const data = await res.json();
      if(!data.success){
        msg.textContent = data.error === 'not_enough_data'
          ? 'No hay suficientes datos en la base para el día de ayer.'
          : 'No se pudo generar el reporte.';
        return;
      }
      msg.textContent = 'Tendencias de ayer (00:00–23:59, hora de Bogotá).';
      const s = buildSeries(data);
      renderCombined(document.getElementById('combinedTrend').getContext('2d'), s);
      renderAQI(document.getElementById('aqiDistribution').getContext('2d'), data.aqi_distribution_7d);
    }catch(err){
      console.error(err);
      msg.textContent = 'Error de red generando el reporte.';
    }
  }

  document.addEventListener('DOMContentLoaded', ()=>{
    const btn = document.getElementById('btnGenerarInterpretacion');
    if(btn) btn.addEventListener('click', generar);
    // Exponer para refresco manual al cambiar marcador, si quisieras:
    window.updateTrendsCharts = ()=>{}; // no autogenerar; solo con el botón
  });
})();
