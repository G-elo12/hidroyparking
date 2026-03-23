// ============================================================
//  Servidor Node.js — WebSocket + Express
//  Sistemas: Hidroponía + Parking
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

// ── Estado Hidroponía ──────────────────────────────────────
let estadoBomba    = false;
let datosSensores  = { temperatura: null, ph: null, nivel: null };
const notificacionesHidro = [];

// ── Estado Parking ─────────────────────────────────────────
const CAPACIDAD_MAX = 50;

let estadoParking = {
  lugares_ocupados: 0,
  capacidad:        CAPACIDAD_MAX,
  pluma:            "arriba",   // "arriba" | "abajo"
  sensor_a:         false,
  sensor_b:         false,
  ultimo_evento:    null,       // "entrada" | "salida"
};

const notificacionesParking = [];
const historialParking = [];    // últimos 20 eventos

// ── WebSocket ──────────────────────────────────────────────
const wss        = new WebSocketServer({ server });
const clienteMap = new Map();

// ── REST: Hidroponía ───────────────────────────────────────

// POST /cmd/hidro  body: { "bomba": true }
app.post("/cmd/hidro", (req, res) => {
  const { bomba } = req.body;
  if (typeof bomba !== "boolean")
    return res.status(400).json({ error: '"bomba" debe ser true o false' });

  estadoBomba = bomba;
  const payload = JSON.stringify({ type: "cmd_hidro", bomba: estadoBomba });
  broadcast(payload, "esp32_hidro");
  broadcast(payload, "dashboard");
  timestamp(`[REST] Bomba hidroponía: ${estadoBomba ? "ENCENDIDA" : "APAGADA"}`);
  res.json({ ok: true, bomba: estadoBomba });
});

// ── REST: Parking ──────────────────────────────────────────

// POST /cmd/parking  body: { "pluma": "arriba" | "abajo" }
app.post("/cmd/parking", (req, res) => {
  const { pluma } = req.body;
  if (pluma !== "arriba" && pluma !== "abajo")
    return res.status(400).json({ error: '"pluma" debe ser "arriba" o "abajo"' });

  ordenarPluma(pluma);
  res.json({ ok: true, pluma });
});

// POST /cmd/parking/reset  — reinicia contador (admin)
app.post("/cmd/parking/reset", (_req, res) => {
  estadoParking.lugares_ocupados = 0;
  estadoParking.ultimo_evento    = null;
  broadcast(JSON.stringify({ type: "parking_state", ...estadoParking }), "dashboard");
  timestamp("[REST] Contador de parking reiniciado");
  res.json({ ok: true, lugares_ocupados: 0 });
});

// ── REST: Estado general ───────────────────────────────────
app.get("/state", (_req, res) => {
  res.json({
    hidro:   { bomba: estadoBomba, sensores: datosSensores },
    parking: estadoParking,
  });
});

app.get("/state/hidro", (_req, res) => {
  res.json({ bomba: estadoBomba, sensores: datosSensores });
});

app.get("/state/parking", (_req, res) => {
  res.json(estadoParking);
});

app.get("/notifications", (_req, res) => {
  res.json({
    hidro:   notificacionesHidro,
    parking: notificacionesParking,
  });
});

app.get("/parking/history", (_req, res) => {
  res.json(historialParking);
});

app.get("/status", (_req, res) => {
  const clientes = [...clienteMap.values()].map(({ device, ip }) => ({ device, ip }));
  res.json({ clientes, total: clienteMap.size });
});

// ── WebSocket: Eventos ─────────────────────────────────────
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

      // ── Registro de dispositivo ──────────────────────────
      case "register": {
        const device = data.device || "unknown";
        clienteMap.set(ws, { device, ip });
        timestamp(`Dispositivo registrado: ${device} (${ip})`);
        enviar(ws, { type: "ack", msg: `Bienvenido, ${device}` });

        if (device === "dashboard") {
          enviar(ws, {
            type:    "state_sync",
            hidro:   { bomba: estadoBomba, sensores: datosSensores },
            parking: estadoParking,
          });
          timestamp(`[SYNC] Estado completo enviado a dashboard (${ip})`);
        }

        if (device === "esp32_parking") {
          // Sincronizar estado de pluma al reconectar
          enviar(ws, { type: "cmd_parking", pluma: estadoParking.pluma });
        }
        break;
      }

      // ── Datos sensores hidroponía ────────────────────────
      case "sensor_data_hidro": {
        const { temperatura, ph, nivel } = data;
        datosSensores = { temperatura, ph, nivel };
        timestamp(`[Hidro] Temp=${temperatura}°C | pH=${ph} | Nivel=${nivel}%`);
        broadcast(
          JSON.stringify({ type: "sensor_data_hidro", sensores: datosSensores }),
          "dashboard"
        );
        evaluarAlertasHidro(temperatura, ph, nivel);
        break;
      }

      // ── Evento de parking ────────────────────────────────
      /*
        El ESP32 detecta el orden de activación:
          A → B  (A primero, luego B) = ENTRADA
          B → A  (B primero, luego A) = SALIDA

        Payload esperado:
        {
          type:     "parking_event",
          evento:   "entrada" | "salida",
          sensor_a: true|false,   // estado actual del sensor A
          sensor_b: true|false    // estado actual del sensor B
        }
      */
      case "parking_event": {
        const { evento, sensor_a, sensor_b } = data;

        estadoParking.sensor_a = sensor_a;
        estadoParking.sensor_b = sensor_b;

        if (evento === "entrada") {
          if (estadoParking.lugares_ocupados < CAPACIDAD_MAX) {
            estadoParking.lugares_ocupados++;
            estadoParking.ultimo_evento = "entrada";
            timestamp(`[Parking] ENTRADA. Ocupados: ${estadoParking.lugares_ocupados}/${CAPACIDAD_MAX}`);
            registrarEventoParking("entrada");
            ordenarPluma("abajo"); // bajar pluma para dejar pasar
          } else {
            timestamp("[Parking] ENTRADA denegada — parking lleno");
            evaluarAlertaParking("full");
          }

        } else if (evento === "salida") {
          if (estadoParking.lugares_ocupados > 0) {
            estadoParking.lugares_ocupados--;
            estadoParking.ultimo_evento = "salida";
            timestamp(`[Parking] SALIDA. Ocupados: ${estadoParking.lugares_ocupados}/${CAPACIDAD_MAX}`);
            registrarEventoParking("salida");
            ordenarPluma("abajo"); // bajar pluma para dejar salir
          } else {
            timestamp("[Parking] SALIDA ignorada — contador ya en 0");
          }
        }

        evaluarAlertaParking(null);

        broadcast(
          JSON.stringify({ type: "parking_state", ...estadoParking }),
          "dashboard"
        );
        break;
      }

      // ── Confirmación estado pluma desde ESP32 ────────────
      case "pluma_status": {
        estadoParking.pluma = data.pluma;
        timestamp(`[Parking] Pluma confirmada: ${data.pluma}`);
        broadcast(
          JSON.stringify({ type: "parking_state", ...estadoParking }),
          "dashboard"
        );
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

// ── Helpers Hidroponía ─────────────────────────────────────
function evaluarAlertasHidro(temperatura, ph, nivel) {
  let alerta = null;
  if (temperatura > 30)           alerta = `Temperatura alta: ${temperatura}°C`;
  else if (ph < 6.0 || ph > 8.0)  alerta = `pH fuera de rango: ${ph}`;
  else if (nivel < 20)             alerta = `Nivel de agua muy bajo: ${nivel}%`;
  if (alerta) agregarNotificacion(notificacionesHidro, alerta, "hidro");
}

// ── Helpers Parking ────────────────────────────────────────
function evaluarAlertaParking(motivo) {
  const ocupados = estadoParking.lugares_ocupados;
  let alerta = null;
  if (motivo === "full") {
    alerta = "Parking lleno — acceso denegado";
  } else if (ocupados >= Math.floor(CAPACIDAD_MAX * 0.9)) {
    alerta = `Parking al ${Math.round((ocupados / CAPACIDAD_MAX) * 100)}% de capacidad`;
  }
  if (alerta) agregarNotificacion(notificacionesParking, alerta, "parking");
}

function registrarEventoParking(tipo) {
  const ev = { tipo, hora: new Date().toISOString(), ocupados: estadoParking.lugares_ocupados };
  historialParking.unshift(ev);
  if (historialParking.length > 20) historialParking.pop();
}

function ordenarPluma(posicion) {
  estadoParking.pluma = posicion;
  const payload = JSON.stringify({ type: "cmd_parking", pluma: posicion });
  broadcast(payload, "esp32_parking");
  broadcast(payload, "dashboard");
  timestamp(`[Parking] Pluma → ${posicion}`);
}

// ── Utilidades ─────────────────────────────────────────────
function agregarNotificacion(lista, mensaje, origen) {
  const n = { id: Date.now(), mensaje, origen, fecha: new Date().toISOString() };
  lista.push(n);
  if (lista.length > 50) lista.shift();
  broadcast(JSON.stringify({ type: "notification", data: n }), "dashboard");
  timestamp(`[ALERTA ${origen.toUpperCase()}] ${mensaje}`);
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

// ── Arrancar ───────────────────────────────────────────────
server.listen(PORT, () => {
  timestamp(`Servidor en http://localhost:${PORT}`);
  timestamp(`WebSocket en ws://localhost:${PORT}`);
  console.log("──────────────────────────────────────────────");
  console.log("  HIDROPONÍA");
  console.log("    POST /cmd/hidro          { bomba: true|false }");
  console.log("    GET  /state/hidro");
  console.log("  PARKING");
  console.log("    POST /cmd/parking        { pluma: 'arriba'|'abajo' }");
  console.log("    POST /cmd/parking/reset");
  console.log("    GET  /state/parking");
  console.log("    GET  /parking/history");
  console.log("  GENERAL");
  console.log("    GET  /state");
  console.log("    GET  /notifications");
  console.log("    GET  /status");
  console.log("──────────────────────────────────────────────");
});