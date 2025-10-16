const API_BASE = '';
const fmt = new Intl.NumberFormat();
const money = new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0});
const $ = (s) => document.querySelector(s);

const chartSeries = new Chart($('#chartSeries'), {
  type: 'line',
  data: { labels: [], datasets: [{ label: 'Trips', data: [], fill: true }] }
});
const chartBorough = new chartSeries($('#chartBorough'), {
  type: 'bar',
  data: { labels: [], datasets: [{ label: 'Trips', data: [] }] }
});

const map = L.map('map').setView([40.73, -73.94], 11);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© OpenStreetMap'
}).addTo(map);
let markers = L.layerGroup().addTo(map);

async function api(path, params) {
  const url = API_BASE + path + (params ? '?' + new URLSearchParams(params) : '');
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function refresh() {
  const query = {
    start: $('#start').value,
    end: $('#end').value,
    vendor_id: $('#vendor').value || undefined,
    passenger_min: $('#pmin').value || undefined,
    passenger_max: $('#pmax').value || undefined
  };
  const m = await api('/api/metrics', query);
  $('#kpiTrips').textContent = fmt.format(m.totalTrips || 0);
  $('#kpiDist').textContent = fmt.format(m.totalDistanceKm || 0) + ' km';
  $('#kpiFare').textContent = money.format(m.avgFare || 0);
  $('#kpiTime').textContent = fmt.format(m.avgTripTimeMin || 0) + ' min';
  chartSeries.data.labels = m.timeSeries?.map(d => d.date) || [];
  chartSeries.data.datasets[0].data = m.timeSeries?.map(d => d.trips) || [];
  chartSeries.update();
  chartBorough.data.labels = m.byBorough?.map(b => b.trips) || [];
  chartBorough.update();

  const heat = await api('/api/geo/heatmap', query);
  markers.clearLayers();
  heat.slice(0, 8000).forEach(p => L.circleMarker([p[0], p[1]], { radius: 2, opacity: .4, fillOpacity: .4}).addTo(markers));

  const t = await api('/api/trips', { offset: 0, limit: 50 });
  const tbody = $('#tripTable tbody');
  tbody.innerHTML = '';
  t.rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.pickup_datetime}</td><td>${r.dropoff_datetime}</td><td>${r.passenger_count}</td>
                    <td>${r.trip_distance_km}</td><td>${money.format(r.fare_amount)}</td>
                    <td>${r.pickup_borough}</td><td>${r.dropoff_borough}</td>`;
    tbody.appendChild(tr);
  });
}

$('#refreshBtn').onClick = refresh;
refresh();