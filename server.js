// ============================================================
//  Servidor Node.js — WebSocket + Express
//  Sistema: Parking (2 plumas independientes) v3.0
//  Instalar: npm install ws express
//  Ejecutar: node server.js
// ============================================================

const express = require("express");
const http    = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;

const app    = express();
const server = http.createServer(app);
app.use(express.json());

// Servir el dashboard directamente si existe index.html
const path = require("path");
app.use(express.static(path.join(__dirname, "public")));

// ── Estado Parking ─────────────────────────────────────────
const CAPACIDAD_MAX = 50;

let estadoParking = {
  lugares_ocupados: 0,
  capacidad:        CAPACIDAD_MAX,
  pluma_entrada:    "arriba",
  pluma_salida:     "arriba",
  sensor_a:         false,
  sensor_b:         false,
  ultimo_evento:    null,
};

const notificacionesParking = [];
const historialParking      = [];  // últimos 20 eventos

// ── WebSocket ──────────────────────────────────────────────
const wss        = new WebSocketServer({ server });
const clienteMap = new Map();

// ═══════════════════════════════════════════════════════════
//  REST — Parking
// ═══════════════════════════════════════════════════════════

// POST /cmd/parking  { "puerta": "entrada"|"salida", "accion": "arriba"|"abajo" }
app.post("/cmd/parking", (req, res) => {
  const { puerta, accion } = req.body;

  if (puerta !== "entrada" && puerta !== "salida")
    return res.status(400).json({ error: '"puerta" debe ser "entrada" o "salida"' });
  if (accion !== "arriba" && accion !== "abajo")
    return res.status(400).json({ error: '"accion" debe ser "arriba" o "abajo"' });

  ordenarPluma(puerta, accion, false); // false = no es sync
  res.json({ ok: true, puerta, accion });
});

// POST /cmd/parking/reset — reinicia contador y cierra plumas
app.post("/cmd/parking/reset", (_req, res) => {
  estadoParking.lugares_ocupados = 0;
  estadoParking.ultimo_evento    = null;

  // FIX: También cierra las plumas al resetear
  estadoParking.pluma_entrada = "arriba";
  estadoParking.pluma_salida  = "arriba";

  // FIX: Broadcast a TODOS (esp32 + dashboard), no solo dashboard
  broadcast(JSON.stringify({ type: "parking_state", ...estadoParking }), "*");

  // Ordenar cierre físico al ESP32
  ordenarPluma("entrada", "arriba", false);
  ordenarPluma("salida",  "arriba", false);

  timestamp("[REST] Contador reiniciado + plumas cerradas");
  res.json({ ok: true, lugares_ocupados: 0 });
});

// ═══════════════════════════════════════════════════════════
//  REST — Estado y utilidades
// ═══════════════════════════════════════════════════════════
app.get("/state",           (_req, res) => res.json(estadoParking));
app.get("/notifications",   (_req, res) => res.json(notificacionesParking));
app.get("/parking/history", (_req, res) => res.json(historialParking));
app.get("/status", (_req, res) => {
  const clientes = [...clienteMap.values()].map(({ device, ip }) => ({ device, ip }));
  res.json({ clientes, total: clienteMap.size });
});

// ═══════════════════════════════════════════════════════════
//  WebSocket — Eventos
// ═══════════════════════════════════════════════════════════
wss.on("connection", (ws, req) => {
  const ip = req.socket.remoteAddress;
  clienteMap.set(ws, { device: "unknown", ip });
  timestamp(`Cliente conectado desde ${ip}. Total: ${wss.clients.size}`);

  ws.on("message", (rawData) => {
    let data;
    try {
      data = JSON.parse(rawData.toString());
    } catch {
      console.error("[JSON] Trama inválida:", rawData.toString());
      return;
    }

    switch (data.type) {

      // ── Registro de dispositivo ────────────────────────
      case "register": {
        const device = data.device || "unknown";
        clienteMap.set(ws, { device, ip });
        timestamp(`Dispositivo registrado: ${device} (${ip})`);
        enviar(ws, { type: "ack", msg: `Bienvenido, ${device}` });

        if (device === "dashboard") {
          // Sincronizar todo el estado al dashboard
          enviar(ws, { type: "state_sync", parking: estadoParking });
          // Enviar historial por WS también
          enviar(ws, { type: "historial_sync", historial: historialParking });
          timestamp(`[SYNC] Estado completo enviado a dashboard (${ip})`);
        }

        if (device === "esp32_parking") {
          // FIX: Marcar como sync=true para que el ESP32 no mueva servos
          enviar(ws, {
            type:  "cmd_parking",
            puerta: "entrada",
            accion: estadoParking.pluma_entrada,
            sync:   true   // El ESP32 actualiza estado interno, no mueve servo
          });
          enviar(ws, {
            type:  "cmd_parking",
            puerta: "salida",
            accion: estadoParking.pluma_salida,
            sync:   true
          });
          // También enviar el estado del contador
          enviar(ws, { type: "parking_state", ...estadoParking });
          timestamp(`[SYNC] Estado completo enviado a esp32_parking (${ip})`);
        }
        break;
      }

      // ── Evento de parking desde ESP32 ─────────────────
      case "parking_event": {
        const { evento, sensor_a, sensor_b } = data;

        estadoParking.sensor_a = sensor_a;
        estadoParking.sensor_b = sensor_b;

        if (evento === "entrada") {
          if (estadoParking.lugares_ocupados < CAPACIDAD_MAX) {
            estadoParking.lugares_ocupados++;
            estadoParking.ultimo_evento = "entrada";
            timestamp(`[Parking] ENTRADA → ${estadoParking.lugares_ocupados}/${CAPACIDAD_MAX}`);
            registrarEventoParking("entrada");
            ordenarPluma("entrada", "abajo", false); // Abrir pluma física
          } else {
            timestamp("[Parking] ENTRADA denegada — parking lleno");
            evaluarAlertaParking("full");
          }

        } else if (evento === "salida") {
          if (estadoParking.lugares_ocupados > 0) {
            estadoParking.lugares_ocupados--;
            estadoParking.ultimo_evento = "salida";
            timestamp(`[Parking] SALIDA → ${estadoParking.lugares_ocupados}/${CAPACIDAD_MAX}`);
            registrarEventoParking("salida");
            ordenarPluma("salida", "abajo", false); // Abrir pluma física
          } else {
            timestamp("[Parking] SALIDA ignorada — contador ya en 0");
          }
        }

        evaluarAlertaParking(null);

        // Broadcast estado actualizado a dashboard
        broadcast(JSON.stringify({ type: "parking_state", ...estadoParking }), "dashboard");
        break;
      }

      // ── Confirmación de pluma desde ESP32 ─────────────
      case "pluma_status": {
        const { puerta, accion } = data;
        if (puerta === "entrada") estadoParking.pluma_entrada = accion;
        else if (puerta === "salida") estadoParking.pluma_salida = accion;

        timestamp(`[Parking] Pluma ${puerta} confirmada: ${accion}`);
        broadcast(JSON.stringify({ type: "parking_state", ...estadoParking }), "dashboard");
        break;
      }

      case "ping":
        enviar(ws, { type: "pong" });
        break;

      default:
        console.log(`[WS] Tipo desconocido: ${data.type}`);
    }
  });

  ws.on("close", (code) => {
    const info = clienteMap.get(ws) || {};
    timestamp(`Desconectado: ${info.device || "unknown"} (${info.ip}) — código ${code}`);
    clienteMap.delete(ws);
  });

  ws.on("error", (err) => console.error("[WS] Error:", err.message));
});

// ═══════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════
function evaluarAlertaParking(motivo) {
  const ocupados = estadoParking.lugares_ocupados;
  let alerta = null;

  if (motivo === "full") {
    alerta = "Parking lleno — acceso denegado";
  } else if (ocupados >= Math.floor(CAPACIDAD_MAX * 0.9)) {
    alerta = `Parking al ${Math.round((ocupados / CAPACIDAD_MAX) * 100)}% de capacidad`;
  }

  if (alerta) agregarNotificacion(alerta);
}

function registrarEventoParking(tipo) {
  const ev = {
    tipo,
    hora:     new Date().toISOString(),
    ocupados: estadoParking.lugares_ocupados
  };
  historialParking.unshift(ev);
  if (historialParking.length > 20) historialParking.pop();

  // FIX: Enviar el evento de historial al dashboard en tiempo real
  broadcast(JSON.stringify({ type: "historial_evento", evento: ev }), "dashboard");
}

// FIX: Agregado parámetro esSync para marcar sincronizaciones iniciales
function ordenarPluma(puerta, accion, esSync = false) {
  if (puerta === "entrada") estadoParking.pluma_entrada = accion;
  else if (puerta === "salida") estadoParking.pluma_salida = accion;

  const payload = JSON.stringify({
    type:  "cmd_parking",
    puerta,
    accion,
    sync:  esSync   // El ESP32 lee esto para no mover servos en sync
  });

  broadcast(payload, "esp32_parking");
  broadcast(payload, "dashboard");
  timestamp(`[Parking] Pluma ${puerta} → ${accion}${esSync ? " (sync)" : ""}`);
}

function agregarNotificacion(mensaje) {
  const n = { id: Date.now(), mensaje, fecha: new Date().toISOString() };
  notificacionesParking.push(n);
  if (notificacionesParking.length > 50) notificacionesParking.shift();
  broadcast(JSON.stringify({ type: "notification", data: n }), "dashboard");
  timestamp(`[ALERTA] ${mensaje}`);
}

function enviar(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(payload, targetDevice = "*") {
  for (const [ws, info] of clienteMap.entries()) {
    if (ws.readyState !== WebSocket.OPEN) continue;
    if (targetDevice !== "*" && info.device !== targetDevice) continue;
    ws.send(payload);
  }
}

function timestamp(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

// ═══════════════════════════════════════════════════════════
//  Arrancar
// ═══════════════════════════════════════════════════════════
server.listen(PORT, () => {
  timestamp(`Servidor en http://localhost:${PORT}`);
  timestamp(`WebSocket en ws://localhost:${PORT}`);
  console.log("──────────────────────────────────────────────────────");
  console.log('  POST /cmd/parking   { puerta, accion }');
  console.log("  POST /cmd/parking/reset");
  console.log("  GET  /state  /notifications  /parking/history  /status");
  console.log("──────────────────────────────────────────────────────");
});
