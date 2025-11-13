// ================================
// CONFIGURACIÓN
// ================================
const apiKey = "0fceb022e90eecf2c580132f9ccd74ce";

const API_BASE = (window.API_BASE || "http://127.0.0.1:5000").replace(/\/$/, "");


// Definir todos los puntos de interés - Aguachica General como principal
const pointsOfInterest = [
    {
        id: 'aguachica_general',
        name: 'Aguachica - Vista General',
        lat: 8.312, // Coordenadas centrales de la ciudad
        lon: -73.626,

        // probar coords bogota 4.707817598852641, -74.06800427097771
        // probar coords medellin 6.247268619172579, -75.57366403882658
        // probar coords cali 3.4457716834629424, -76.53323977739839


        isMain: true, // Punto principal para el dashboard
        description: 'Datos generales de calidad del aire para toda la ciudad de Aguachica'
    },
    {
        id: 'parque_central',
        name: 'Parque Central',
        lat: 8.310675833008426,
        lon: -73.62363665855918,
        isMain: false,
        description: 'Zona comercial y administrativa central'
    },
    {
        id: 'universidad',
        name: 'Universidad Popular del Cesar',
        lat: 8.314789098234467,
        lon: -73.59638568793966,
        isMain: false,
        description: 'Campus universitario y zona educativa'
    },
    {
        id: 'parque_morrocoy',
        name: 'Parque Morrocoy',
        lat: 8.310373774726447,
        lon: -73.61670782048647,
        isMain: false,
        description: 'Área recreativa y residencial'
    },
    {
        id: 'patinodromo',
        name: 'Patinódromo',
        lat: 8.297149888853758,
        lon: -73.62335200184627,
        isMain: false,
        description: 'Zona deportiva y recreativa sur'
    },
    {
        id: 'ciudadela_paz',
        name: 'Ciudadela de la Paz',
        lat: 8.312099985681844,
        lon: -73.63467832511535,
        isMain: false,
        description: 'Sector residencial oeste'
    },
    {
        id: 'bosque',
        name: 'Bosque',
        lat: 8.312303609676293,
        lon: -73.61448867800057,
        isMain: false,
        description: 'Área verde y conservación ambiental'
    },
    {
        id: 'estadio',
        name: 'Estadio',
        lat: 8.30159931733102,
        lon: -73.622763654179,
        isMain: false,
        description: 'Complejo deportivo municipal'
    }
];

let selectedPointId = null;
// Variable global para almacenar datos de todos los puntos
let allPointsData = new Map();
let map;
let markers = new Map();

// ================================
// FUNCIONES AUXILIARES
// ================================

// Traducción AQI
function getAQIMessage(aqi) {
    if (aqi >= 151) return { msg: "Muy mala", cls: "bg-red-200", color: "#dc2626" };
    if (aqi >= 101) return { msg: "No saludable para grupos sensibles", cls: "bg-yellow-200", color: "#d97706" };
    if (aqi >= 51) return { msg: "Moderada", cls: "bg-orange-200", color: "#ea580c" };
    return { msg: "Buena", cls: "bg-green-200", color: "#16a34a" };
}

// Color para heatmap
function getColor(value) {
    if (value <= 25) return "bg-green-200";
    if (value <= 50) return "bg-yellow-200";
    if (value <= 100) return "bg-orange-300";
    if (value <= 150) return "bg-red-300";
    return "bg-red-500";
}

// Generar URLs para un punto específico
function generateUrls(lat, lon) {
    const now = Math.floor(Date.now() / 1000);
    const fiveDaysAgo = now - 5 * 24 * 60 * 60;

    return {
        airQuality: `https://api.openweathermap.org/data/2.5/air_pollution?lat=${lat}&lon=${lon}&appid=${apiKey}`,
        weather: `https://api.openweathermap.org/data/2.5/weather?lat=${lat}&lon=${lon}&appid=${apiKey}&units=metric&lang=es`,
        forecast: `https://api.openweathermap.org/data/2.5/air_pollution/forecast?lat=${lat}&lon=${lon}&appid=${apiKey}`,
        history: `https://api.openweathermap.org/data/2.5/air_pollution/history?lat=${lat}&lon=${lon}&start=${fiveDaysAgo}&end=${now}&appid=${apiKey}`
    };
}

// ================================
// FUNCIÓN PARA OBTENER DATOS DE UN PUNTO
// ================================
async function getPointData(point) {
    const urls = generateUrls(point.lat, point.lon);

    try {
        const [airQualityResponse, weatherResponse, forecastResponse] = await Promise.all([
            fetch(urls.airQuality),
            fetch(urls.weather),
            fetch(urls.forecast)
        ]);

        if (!airQualityResponse.ok || !weatherResponse.ok || !forecastResponse.ok) {
            throw new Error(`Error al obtener datos para ${point.name}`);
        }

        const airQualityData = await airQualityResponse.json();
        const weatherData = await weatherResponse.json();
        const forecastData = await forecastResponse.json();

        // Histórico opcional
        let historyData = { list: [] };
        try {
            const r = await fetch(urls.history);
            if (r.ok) historyData = await r.json();
        } catch (e) {
            console.warn(`No se pudo obtener histórico para ${point.name}:`, e);
        }

        return {
            point,
            airQuality: airQualityData,
            weather: weatherData,
            forecast: forecastData,
            history: historyData
        };
    } catch (error) {
        console.error(`Error al obtener datos para ${point.name}:`, error);
        return null;
    }
}

// ================================
// FUNCIÓN PARA CARGAR TODOS LOS PUNTOS
// ================================
async function loadAllPointsData() {
    console.log("Cargando datos para todos los puntos...");

    // Actualizar estado de carga
    if (typeof updateLoadingStatus === 'function') {
        updateLoadingStatus("Obteniendo datos de calidad del aire...");
    }

    // Mostrar indicador de carga
    document.getElementById("pm25").innerText = "Cargando...";
    document.getElementById("pm10").innerText = "Cargando...";
    document.getElementById("temperature").innerText = "Cargando...";
    document.getElementById("qualityMessage").innerText = "Cargando...";

    try {
        // Cargar datos de todos los puntos en paralelo
        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus("Consultando API para 8 puntos de interés...");
        }

        const promises = pointsOfInterest.map(point => getPointData(point));
        const results = await Promise.all(promises);

        // Almacenar datos válidos
        results.forEach(result => {
            if (result) {
                allPointsData.set(result.point.id, result);
            }
        });

        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus("Actualizando marcadores del mapa...");
        }

        // Actualizar marcadores en el mapa
        updateMapMarkers();

        // Mostrar datos del punto principal en el dashboard
        const mainPoint = pointsOfInterest.find(p => p.isMain);
        if (mainPoint && allPointsData.has(mainPoint.id)) {
            updateDashboard(allPointsData.get(mainPoint.id));
        }

        // Finalizar carga
        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus(`Datos cargados correctamente para ${allPointsData.size} puntos`, true);
        }

        console.log("Datos cargados para", allPointsData.size, "puntos");

    } catch (error) {
        console.error("Error al cargar datos de los puntos:", error);
        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus("Error al cargar datos");
        }
        showErrorState();
    }
}

// ================================
// FUNCIÓN PARA ACTUALIZAR MARCADORES
// ================================
function updateMapMarkers() {
    // Limpiar marcadores existentes
    markers.forEach(marker => {
        map.removeLayer(marker);
    });
    markers.clear();

    // Agregar marcadores actualizados
    pointsOfInterest.forEach(point => {
        const data = allPointsData.get(point.id);
        let popupContent;

        if (data) {
            const pm25 = data.airQuality.list[0].components.pm2_5;
            const pm10 = data.airQuality.list[0].components.pm10;
            const aqi = data.airQuality.list[0].main.aqi;
            const temp = data.weather.main.temp;
            const { msg, color } = getAQIMessage(aqi);

            popupContent = `
                <div style="min-width: 220px;">
                    <h4 style="margin: 0 0 10px 0; color: #1f2937; font-weight: bold;">${point.name}</h4>
                    <p style="margin: 0 0 8px 0; font-size: 11px; color: #6b7280;">${point.description}</p>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 12px;">
                        <div><strong>PM2.5:</strong> ${pm25.toFixed(1)} µg/m³</div>
                        <div><strong>PM10:</strong> ${pm10.toFixed(1)} µg/m³</div>
                        <div><strong>AQI:</strong> ${aqi}</div>
                        <div><strong>Temp:</strong> ${temp.toFixed(1)}°C</div>
                    </div>
                    <div style="margin-top: 8px; padding: 4px 8px; background-color: ${color}20; border-radius: 4px; text-align: center;">
                        <strong style="color: ${color};">${msg}</strong>
                    </div>
                    <button onclick="showPointDetails('${point.id}')" 
                            style="width: 100%; margin-top: 8px; padding: 6px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                        ${point.isMain ? 'Ver Vista General' : 'Ver Detalles'}
                    </button>
                </div>
            `;
        } else {
            popupContent = `
                <div>
                    <h4 style="margin: 0 0 8px 0;">${point.name}</h4>
                    <p style="color: #ef4444; margin: 0;">Error al cargar datos</p>
                    <button onclick="retryPointData('${point.id}')" 
                            style="width: 100%; margin-top: 8px; padding: 4px; background: #ef4444; color: white; border: none; border-radius: 4px; cursor: pointer;">
                        Reintentar
                    </button>
                </div>
            `;
        }

        const marker = L.marker([point.lat, point.lon])
            .bindPopup(popupContent)
            .addTo(map);

        // Marcar el punto principal (Aguachica General) con un icono especial
        if (point.isMain) {
            marker.setIcon(L.icon({
                iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-gold.png',
                shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                iconSize: [25, 41],
                iconAnchor: [12, 41],
                popupAnchor: [1, -34],
                shadowSize: [41, 41]
            }));
        } else {
            // Usar iconos azules para puntos específicos
            marker.setIcon(L.icon({
                iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-blue.png',
                shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                iconSize: [25, 41],
                iconAnchor: [12, 41],
                popupAnchor: [1, -34],
                shadowSize: [41, 41]
            }));
        }

        markers.set(point.id, marker);
    });
}

// ================================
// FUNCIÓN PARA ACTUALIZAR DASHBOARD
// ================================
function updateDashboard(data) {
    if (!data) return;

    const pm25 = data.airQuality.list[0].components.pm2_5;
    const pm10 = data.airQuality.list[0].components.pm10;
    const temperature = data.weather.main.temp;
    const aqi = data.airQuality.list[0].main.aqi;

    const { msg, cls } = getAQIMessage(aqi);

    document.getElementById("pm25").innerText = `${pm25.toFixed(2)} µg/m³`;
    document.getElementById("pm10").innerText = `${pm10.toFixed(2)} µg/m³`;
    document.getElementById("temperature").innerText = `${temperature.toFixed(1)} °C`;
    document.getElementById("qualityMessage").innerText = msg;
    document.getElementById("airQuality").className = `rounded-xl p-4 shadow ${cls}`;

    // Actualizar información del mapa según el punto seleccionado
    updateMapInfo(data.point);

    // Actualizar otros componentes del dashboard
    renderHeatmap(data.forecast);
    renderForecastSummary(data.forecast);
    generarBoxplots({ history: data.history, forecast: data.forecast });
    detectarAnomalias(data.history);
    updateDailySummary(pm25, pm10, aqi);
}

// ================================
// FUNCIÓN PARA ACTUALIZAR INFO DEL MAPA
// ================================
function updateMapInfo(point) {
    const locationElement = document.querySelector('p');
    const coordsElement = document.querySelectorAll('p')[1];

    if (point.isMain) {
        if (locationElement) locationElement.innerHTML = '📍 Ubicación: Aguachica, Cesar (Vista General)';
        if (coordsElement) coordsElement.innerHTML = `🗺️ Coordenadas: ${point.lat}, ${point.lon}`;
    } else {
        if (locationElement) locationElement.innerHTML = `📍 Ubicación: ${point.name}, Aguachica, Cesar`;
        if (coordsElement) coordsElement.innerHTML = `🗺️ Coordenadas: ${point.lat.toFixed(6)}, ${point.lon.toFixed(6)}`;
    }
}

// ================================
// FUNCIONES GLOBALES PARA INTERACCIÓN
// ================================
window.showPointDetails = function (pointId) {
    const data = allPointsData.get(pointId);
    if (!data) return;

    // Guardar selección actual
    selectedPointId = pointId;

    // Actualizar selección visual en el sidebar (si existe la función en index.html)
    if (typeof updatePointSelection === 'function') {
        updatePointSelection(pointId);
    }

    // Actualizar textos fijos (Ubicación y Coordenadas) SOLO al cambiar de punto
    try {
        if (typeof updateMapInfo === 'function') {
            updateMapInfo(data.point);
        }
    } catch (e) {
        console.warn('No se pudo actualizar info de mapa para', pointId, e);
    }

    // Enfocar el mapa en el marcador y abrir su popup
    try {
        const marker = markers && markers.get ? markers.get(pointId) : null;
        if (marker && map) {
            const latlng = marker.getLatLng();
            map.setView(latlng, Math.max(map.getZoom(), 15), { animate: true });
            marker.openPopup();
        }
    } catch (e) {
        console.warn('No se pudo enfocar el mapa para', pointId, e);
    }

    // Actualizar dashboard con datos del punto seleccionado (sin tocar ubicación/coords)
    updateDashboard(data);

    // Actualizar título
    const titleElement = document.querySelector('h1');
    if (data.point.isMain) {
        titleElement.textContent = 'Calidad del Aire - Aguachica (Vista General)';
    } else {
        titleElement.textContent = `Calidad del Aire - ${data.point.name}`;
    }

    // Scroll hacia el dashboard
    document.querySelector('main').scrollIntoView({ behavior: 'smooth' });
};

window.retryPointData = async function (pointId) {
    const point = pointsOfInterest.find(p => p.id === pointId);
    if (!point) return;

    console.log(`Reintentando cargar datos para ${point.name}`);

    // Mostrar indicador de carga en el estado
    if (typeof updateLoadingStatus === 'function') {
        updateLoadingStatus(`Reintentando datos para ${point.name}...`);
    }

    const data = await getPointData(point);

    if (data) {
        allPointsData.set(pointId, data);
        updateMapMarkers();

        // Si es el punto actualmente seleccionado, actualizar dashboard
        const currentTitle = document.querySelector('h1').textContent;
        if (currentTitle.includes(point.name) || (point.isMain && currentTitle.includes('Vista General'))) {
            updateDashboard(data);
        }

        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus(`Datos actualizados para ${point.name}`, true);
        }

        console.log(`Datos actualizados para ${point.name}`);
    } else {
        if (typeof updateLoadingStatus === 'function') {
            updateLoadingStatus(`Error al reintentar datos para ${point.name}`);
        }
    }
};

// Función para mostrar la vista general de Aguachica
window.showGeneralView = function () {
    const mainPoint = pointsOfInterest.find(p => p.isMain);
    if (mainPoint && allPointsData.has(mainPoint.id)) {
        showPointDetails(mainPoint.id);
    }
};

// Función para obtener resumen de todos los puntos
window.getAllPointsSummary = function () {
    const summary = Array.from(allPointsData.values()).map(data => {
        const pm25 = data.airQuality.list[0].components.pm2_5;
        const pm10 = data.airQuality.list[0].components.pm10;
        const aqi = data.airQuality.list[0].main.aqi;
        const temp = data.weather.main.temp;

        return {
            name: data.point.name,
            pm25: pm25.toFixed(1),
            pm10: pm10.toFixed(1),
            aqi,
            temp: temp.toFixed(1),
            quality: getAQIMessage(aqi).msg
        };
    });

    console.table(summary);
    return summary;
};

// ================================
// FUNCIONES DE VISUALIZACIÓN (mantener las existentes)
// ================================
function renderHeatmap(forecastData) {
    const tbody = document.getElementById("heatmapTable").getElementsByTagName("tbody")[0];
    tbody.innerHTML = "";

    const now = new Date();
    const currentHour = now.getHours();

    const intervals = [];
    for (let i = 0; i < 6; i++) {
        intervals.push((currentHour + i * 2) % 24);
    }

    const filteredData = (forecastData.list || []).filter((data) => {
        const fecha = new Date(data.dt * 1000);
        const hour = fecha.getHours();
        return intervals.includes(hour);
    });

    const uniqueData = [];
    filteredData.forEach((data) => {
        const hour = new Date(data.dt * 1000).getHours();
        if (!uniqueData.some((d) => new Date(d.dt * 1000).getHours() === hour)) {
            uniqueData.push(data);
        }
    });

    uniqueData.forEach((data) => {
        const fecha = new Date(data.dt * 1000);
        const { pm2_5, pm10, o3, no2 } = data.components;

        const row = tbody.insertRow();
        row.insertCell(0).innerText = fecha.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
        row.insertCell(1).innerText = pm2_5.toFixed(2);
        row.insertCell(2).innerText = pm10.toFixed(2);
        row.insertCell(3).innerText = o3.toFixed(2);
        row.insertCell(4).innerText = no2.toFixed(2);

        row.cells[1].classList.add(getColor(pm2_5));
        row.cells[2].classList.add(getColor(pm10));
        row.cells[3].classList.add(getColor(o3));
        row.cells[4].classList.add(getColor(no2));
    });

    if (uniqueData.length === 0) {
        tbody.innerHTML = "<tr><td colspan='5'>No hay pronóstico disponible para hoy.</td></tr>";
    }
}

function renderForecastSummary(forecastData) {
    const forecastDiv = document.getElementById("forecastMessage");

    if (!forecastData.list || forecastData.list.length === 0) {
        forecastDiv.innerText = "No hay suficiente pronóstico disponible para generar un resumen.";
        return;
    }

    const now = new Date();
    const currentHour = now.getHours();
    const intervals = [];
    for (let i = 0; i < 6; i++) {
        intervals.push((currentHour + i * 2) % 24);
    }

    const data = forecastData.list.filter((d) =>
        intervals.includes(new Date(d.dt * 1000).getHours())
    );

    if (data.length === 0) {
        forecastDiv.innerText = "No hay suficiente pronóstico disponible para generar un resumen.";
        return;
    }

    let sumPm25 = 0, sumPm10 = 0, sumO3 = 0, sumNo2 = 0;
    data.forEach(d => {
        sumPm25 += d.components.pm2_5;
        sumPm10 += d.components.pm10;
        sumO3 += d.components.o3;
        sumNo2 += d.components.no2;
    });

    const avgPm25 = sumPm25 / data.length;
    const avgPm10 = sumPm10 / data.length;
    const avgO3 = sumO3 / data.length;
    const avgNo2 = sumNo2 / data.length;

    let mensaje = "✅ Buen día para actividades al aire libre.";
    let clases = "mt-4 text-base font-medium text-center text-green-700 bg-green-100 p-3 rounded-lg";

    if (avgPm25 > 35 || avgPm10 > 50 || avgO3 > 100 || avgNo2 > 200) {
        mensaje = "⚠️ Precaución: se esperan niveles moderados de contaminación.";
        clases = "mt-4 text-base font-medium text-center text-yellow-700 bg-yellow-100 p-3 rounded-lg";
    }
    if (avgPm25 > 55 || avgPm10 > 100 || avgO3 > 150 || avgNo2 > 300) {
        mensaje = "❌ Mala calidad del aire prevista. Mejor evitar actividades al aire libre.";
        clases = "mt-4 text-base font-medium text-center text-red-700 bg-red-100 p-3 rounded-lg";
    }

    forecastDiv.innerText = mensaje;
    forecastDiv.className = clases;
}

// Estadísticas para boxplots
function calcularEstadisticas(valores) {
    if (!valores || valores.length === 0) return { mediana: "-", q1: "-", q3: "-", min: "-", max: "-" };

    const sorted = [...valores].sort((a, b) => a - b);
    const n = sorted.length;

    const percentile = (p) => {
        const index = (p / 100) * (n - 1);
        const lower = Math.floor(index);
        const upper = Math.ceil(index);
        return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
    };

    return {
        mediana: percentile(50).toFixed(2),
        q1: percentile(25).toFixed(2),
        q3: percentile(75).toFixed(2),
        min: sorted[0].toFixed(2),
        max: sorted[n - 1].toFixed(2)
    };
}


async function generarBoxplots(sources) {
    const tableBody = document.querySelector("#boxplotTable tbody");
    tableBody.innerHTML = "<tr><td colspan='12'>Cargando datos de la base de datos...</td></tr>";

    try {
        // Obtener el punto actual seleccionado
        const currentPointId = selectedPointId || 'aguachica_general';

        // Consultar datos históricos de los últimos 365 días desde tu base de datos
        // Consultar datos históricos de los últimos 365 días desde tu base de datos
        const response = await fetch(
            `${API_BASE}/api/historical/${currentPointId}?days=365&limit=50000`
        );


        if (!response.ok) {
            throw new Error('No se pudieron obtener datos de la base de datos');
        }

        const result = await response.json();

        if (!result.success || !result.data || result.data.length === 0) {
            tableBody.innerHTML = `<tr><td colspan="12">No hay datos históricos en la base de datos para este punto.</td></tr>`;
            return;
        }

        // Agrupar por mes
        const agrupado = new Map();

        result.data.forEach((entry) => {
            const fecha = new Date(entry.timestamp);
            const mesIndex = fecha.getMonth();
            const mesNombre = fecha.toLocaleString("es-ES", { month: "long" });

            if (!agrupado.has(mesIndex)) {
                agrupado.set(mesIndex, { nombre: mesNombre, pm25: [], pm10: [], aqi: [] });
            }

            const bucket = agrupado.get(mesIndex);

            // Agregar valores válidos
            if (entry.pm2_5 != null && !isNaN(entry.pm2_5)) {
                bucket.pm25.push(Number(entry.pm2_5));
            }
            if (entry.pm10 != null && !isNaN(entry.pm10)) {
                bucket.pm10.push(Number(entry.pm10));
            }
            if (entry.aqi != null && !isNaN(entry.aqi)) {
                bucket.aqi.push(Number(entry.aqi));
            }
        });

        const currentMonthIndex = new Date().getMonth();
        tableBody.innerHTML = "";

        // Mostrar desde enero hasta el mes ANTERIOR (sin incluir el mes actual)
        const mesesParaMostrar = Array.from({ length: currentMonthIndex }, (_, i) => i);

        if (agrupado.size === 0) {
            tableBody.innerHTML = `<tr><td colspan="12">Sin datos suficientes para calcular boxplots.</td></tr>`;
            return;
        }

        mesesParaMostrar.forEach((mesIndex) => {
            let mesData = agrupado.get(mesIndex);

            // Solo mostrar fila si hay datos para ese mes
            if (mesData && (mesData.pm25.length > 0 || mesData.pm10.length > 0)) {
                const statsPm25 = calcularEstadisticas(mesData.pm25);
                const statsPm10 = calcularEstadisticas(mesData.pm10);
                const mean = (arr) => (arr && arr.length) ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(2) : "-";
                const meanAqi = mean(mesData.aqi);

                const row = document.createElement("tr");
                row.innerHTML = `
                <td>${mesData.nombre.charAt(0).toUpperCase() + mesData.nombre.slice(1)}</td>
                <td>${statsPm25.mediana}</td>
                <td>${statsPm25.q1}</td>
                <td>${statsPm25.q3}</td>
                <td>${statsPm25.min}</td>
                <td>${statsPm25.max}</td>
                <td>${statsPm10.mediana}</td>
                <td>${statsPm10.q1}</td>
                <td>${statsPm10.q3}</td>
                <td>${statsPm10.min}</td>
                <td>${statsPm10.max}</td>
                <td>${meanAqi}</td>
                `;
                tableBody.appendChild(row);

                // Debug: mostrar en consola
                console.log(`${mesData.nombre}: PM2.5 valores = ${mesData.pm25.length}, Max = ${statsPm25.max}`);
            }
        });

        // Si no se agregó ninguna fila
        if (tableBody.children.length === 0) {
            tableBody.innerHTML = `<tr><td colspan="12">Sin datos suficientes para calcular boxplots.</td></tr>`;
        }

    } catch (error) {
        console.error('Error al generar boxplots desde DB:', error);
        tableBody.innerHTML = `<tr><td colspan="12">Error al cargar datos: ${error.message}</td></tr>`;
    }
}


function detectarAnomalias(historyData) {
    const ul = document.querySelector("#anomaliasList");
    ul.innerHTML = "";

    if (!historyData || !historyData.list || historyData.list.length === 0) {
        ul.innerHTML = "<li class='text-gray-500'>No hay datos históricos suficientes para detectar anomalías.</li>";
        return;
    }

    const now = Date.now();
    const eventos = [];

    historyData.list.forEach((entry, i) => {
        const fecha = new Date(entry.dt * 1000);
        if (fecha.getTime() > now) return;

        const { pm2_5, pm10, o3, no2 } = entry.components;

        if (pm2_5 > 35) eventos.push({ fecha, desc: `Pico inusual de PM₂.₅ (${pm2_5.toFixed(1)} µg/m³)` });
        if (pm10 > 50) eventos.push({ fecha, desc: `Pico elevado de PM₁₀ (${pm10.toFixed(1)} µg/m³)` });
        if (o3 > 100) eventos.push({ fecha, desc: `Concentración alta de O₃ (${o3.toFixed(1)} µg/m³)` });
        if (no2 > 200) eventos.push({ fecha, desc: `Concentración elevada de NO₂ (${no2.toFixed(1)} µg/m³)` });

        if (i > 0) {
            const prev = historyData.list[i - 1].components;
            if (Math.abs(pm2_5 - prev.pm2_5) > 20) eventos.push({ fecha, desc: `Salto brusco en PM₂.₅` });
            if (Math.abs(pm10 - prev.pm10) > 25) eventos.push({ fecha, desc: `Salto brusco en PM₁₀` });
            if (Math.abs(o3 - prev.o3) > 30) eventos.push({ fecha, desc: `Variación repentina en O₃` });
            if (Math.abs(no2 - prev.no2) > 40) eventos.push({ fecha, desc: `Variación repentina en NO₂` });
        }
    });

    if (eventos.length === 0) {
        ul.innerHTML = "<li class='text-gray-500'>No se detectaron anomalías recientes.</li>";
        return;
    }

    eventos.slice(-5).forEach(e => {
        const li = document.createElement("li");
        li.innerHTML = `<strong>${e.fecha.toLocaleString("es-ES")}</strong>: ${e.desc}`;
        ul.appendChild(li);
    });
}

function showErrorState() {
    document.getElementById("pm25").innerText = "Error al obtener datos";
    document.getElementById("pm10").innerText = "Error al obtener datos";
    document.getElementById("temperature").innerText = "Error";
    document.getElementById("qualityMessage").innerText = "Error en AQI";
    document.getElementById("heatmapTable").innerHTML =
        "<tr><td colspan='5'>Error al obtener datos horarios.</td></tr>";

    const tb = document.querySelector("#boxplotTable tbody");
    if (tb) tb.innerHTML = "<tr><td colspan='11'>No fue posible cargar los boxplots.</td></tr>";
}

// ================================

// ================================
// RESUMEN DEL DÍA
// ================================
function updateDailySummary(pm25, pm10, aqi) {
    const summaryEl = document.getElementById('dailySummary');
    const adviceEl = document.getElementById('dailyAdvice');
    const badgeEl = document.getElementById('summaryBadge');
    if (!summaryEl || !adviceEl || !badgeEl) return;

    // Mensaje base por categoría de AQI (1=buena..5=muy mala para OpenWeather)
    const cat = (aqi >= 5) ? 5 : (aqi <= 1 ? 1 : aqi);
    const map = {
        1: {
            title: 'Buena', text: 'El aire es adecuado para actividades al aire libre.',
            tips: ['Ideal para ejercicio al aire libre.', 'OK.']
        },
        2: {
            title: 'Aceptable', text: 'La calidad es aceptable; algunas personas muy sensibles podrían notar molestias.',
            tips: ['Si eres sensible, limita esfuerzos intensos prolongados.']
        },
        3: {
            title: 'Moderada', text: 'La calidad del aire podría afectar a grupos sensibles.',
            tips: ['Personas con asma o EPOC: lleven medicación.', 'Considera ejercicio moderado.']
        },
        4: {
            title: 'Mala', text: 'El aire no es saludable; minimiza la exposición.',
            tips: ['Reduce ejercicio intenso al aire libre.', 'Utiliza mascarilla si presentas síntomas.']
        },
        5: {
            title: 'Muy mala', text: 'Evita actividades al aire libre y protege a grupos sensibles.',
            tips: ['Permanece en interiores si es posible.', 'Usa purificador o ventilación con filtros.']
        },
    };

    const { title, text, tips } = map[cat];
    badgeEl.textContent = title;

    // Construir narrativo corto con contaminante dominante
    let dominant = 'PM2.5';
    if (pm10 > pm25) dominant = 'PM10';
    const dominantStr = `El contaminante dominante hoy es <b>${dominant}</b>.`;

    summaryEl.innerHTML = `${text} ${dominantStr}`;

    // Consejos
    adviceEl.innerHTML = '';
    tips.forEach(t => {
        const li = document.createElement('li');
        li.textContent = t;
        adviceEl.appendChild(li);
    });
}

//pave si funciona
// ===== Tendencias y distribución (añadir al final de script.js) =====
(function () {
    const TRENDS_DAYS = 7;
    const LIMIT = 5000;
    let pm25Chart, pm10Chart, aqiChart;

    function getSelectedLocationId() {
        // 1) Si tu app ya guarda el punto actual en window.currentPointId, úsalo
        if (window.currentPointId) return window.currentPointId;
        // 2) Si no, lo inferimos del item activo del sidebar (sin tocar tu JS)
        const active = document.querySelector('.point-item.active');
        if (active) {
            const onclick = active.getAttribute('onclick') || '';
            const m = onclick.match(/showPointDetails\('([^']+)'\)/);
            if (m && m[1]) return m[1];
        }
        // Fallback
        return 'aguachica_general';
    }

    async function fetchHistorical(locationId) {
        const url = `${API_BASE}/api/historical/${encodeURIComponent(locationId)}?days=${TRENDS_DAYS}&limit=${LIMIT}`;
        const r = await fetch(url);
        const j = await r.json();
        if (!j.success) throw new Error(j.error || 'No data');
        return j.data || [];
    }

    function last24h(data) {
        const now = Date.now();
        const from = now - 24 * 3600 * 1000;
        return data.filter(d => new Date(d.timestamp).getTime() >= from);
    }

    function buildSeries(data, field) {
        const arr = data
            .map(d => ({ t: new Date(d.timestamp), v: d[field] }))
            .filter(p => p.v != null && !isNaN(p.v))
            .sort((a, b) => a.t - b.t);

        return {
            labels: arr.map(p =>
                p.t.toLocaleTimeString('es-CO', { hour: '2-digit', minute: '2-digit' })
            ),
            values: arr.map(p => +Number(p.v).toFixed(2)),
        };
    }

    function groupAQICategories(data) {
        // OpenWeather AQI: 1..5 (1=Bueno ... 5=Muy malo)  :contentReference[oaicite:6]{index=6}
        const labels = ['Bueno', 'Aceptable', 'Moderado', 'Malo', 'Muy malo'];
        const counts = [0, 0, 0, 0, 0];
        data.forEach(d => {
            const a = Number(d.aqi);
            if (a >= 1 && a <= 5) counts[a - 1]++;
        });
        return { labels, values: counts };
    }

    function showTrendsSection() {
        const sec = document.getElementById('trendsSection');
        if (sec) sec.classList.remove('hidden');
    }

    function destroyIf(chart) { if (chart && chart.destroy) chart.destroy(); }

    async function renderTrends() {
        const btn = document.getElementById('btnInterpretacion');
        try {
            if (btn) { btn.disabled = true; btn.textContent = 'Generando...'; }

            const locationId = getSelectedLocationId();
            const data = await fetchHistorical(locationId);
            const data24 = last24h(data);

            const s25 = buildSeries(data24, 'pm2_5');
            const s10 = buildSeries(data24, 'pm10');

            showTrendsSection();

            const ctx25 = document.getElementById('trendPM25').getContext('2d');
            const ctx10 = document.getElementById('trendPM10').getContext('2d');
            const ctxAqi = document.getElementById('aqiPie').getContext('2d');

            destroyIf(pm25Chart); destroyIf(pm10Chart); destroyIf(aqiChart);

            pm25Chart = new Chart(ctx25, {
                type: 'line',
                data: { labels: s25.labels, datasets: [{ label: 'PM₂.₅ (µg/m³)', data: s25.values, tension: 0.3, fill: false, borderWidth: 2 }] },
                options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } }, plugins: { legend: { display: false } } }
            });

            pm10Chart = new Chart(ctx10, {
                type: 'line',
                data: { labels: s10.labels, datasets: [{ label: 'PM₁₀ (µg/m³)', data: s10.values, tension: 0.3, fill: false, borderWidth: 2 }] },
                options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } }, plugins: { legend: { display: false } } }
            });

            const aqi = groupAQICategories(data);
            aqiChart = new Chart(ctxAqi, {
                type: 'doughnut',
                data: { labels: aqi.labels, datasets: [{ data: aqi.values }] },
                options: { responsive: true, maintainAspectRatio: false }
            });

            // scroll suave hasta la sección
            document.getElementById('trendsSection').scrollIntoView({ behavior: 'smooth', block: 'start' });
        } catch (err) {
            console.error('Tendencias:', err);
            alert('No se pudieron generar las tendencias. Revisa que haya datos históricos suficientes.');
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Generar Interpretación'; }
        }
    }

    // Click en el botón
    document.addEventListener('DOMContentLoaded', () => {
        const btn = document.getElementById('btnInterpretacion');
        if (btn) btn.addEventListener('click', renderTrends);
    });
})();




// INICIALIZACIÓN DEL MAPA
// ================================
function initializeMap() {
    // Crear el mapa centrado en Aguachica
    map = L.map('map').setView([8.312, -73.626], 13);

    // Cargar tiles de OpenStreetMap
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap'
    }).addTo(map);
}

// ================================
// INICIO
// ================================
window.onload = function () {
    initializeMap();
    loadAllPointsData();
};