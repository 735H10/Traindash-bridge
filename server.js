/**
 * TrainDash Bridge — Darwin Kafka → REST API
 * Deployed on Railway.app
 *
 * Credentials are loaded from environment variables (set in Railway dashboard)
 * — never hardcoded here since this file lives in a public GitHub repo.
 */

const { Kafka }  = require('kafkajs');
const express    = require('express');
const cors       = require('cors');
const zlib       = require('zlib');

// ── Config from environment variables ────────────────
// These are set in Railway dashboard → Variables tab
const CONFIG = {
  kafka: {
    broker:        process.env.KAFKA_BROKER        || 'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092',
    topic:         process.env.KAFKA_TOPIC         || 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON',
    consumerGroup: process.env.KAFKA_GROUP         || 'SC-c8a3c6c8-2c1a-4063-9e5f-55beb9da3309',
    username:      process.env.KAFKA_USERNAME      || '',
    password:      process.env.KAFKA_PASSWORD      || '',
  },
  port:          parseInt(process.env.PORT) || 3000,   // Railway sets PORT automatically
  watchStations: (process.env.WATCH_STATIONS || 'GLD,WOK,WAT,LBG,CLJ,VIC').split(','),
};

// Validate credentials on boot
if (!CONFIG.kafka.username || !CONFIG.kafka.password) {
  console.error('❌ KAFKA_USERNAME and KAFKA_PASSWORD environment variables are required.');
  console.error('   Set them in Railway → your service → Variables tab.');
  process.exit(1);
}

// ── TIPLOC → CRS mapping ──────────────────────────────
// Darwin uses internal TIPLOC codes. This maps them to the CRS codes
// that TrainDash uses. Covers most major stations — extend as needed.
const TIPLOC_TO_CRS = {
  // London Terminals
  'WATRLMN':'WAT', 'VAUXHAL':'VXH',  'CLPHMJN':'CLJ', 'VICTRIA':'VIC',
  'PADTON': 'PAD', 'KNGX':   'KGX',  'EUSTON': 'EUS', 'STPX':   'STP',
  'STPXBOX':'STP', 'LIVST':  'LST',  'CANNON': 'CST', 'LNDNBDG':'LBG',
  'CHARING':'CHX', 'BLKFRAS':'BFR',  'MRYBONE':'MYB',
  // South West / Surrey
  'GUILDFD':'GLD', 'WOKING': 'WOK',  'BSINGSTK':'BSK','WINCHTR':'WIN',
  'SOTON':  'SOU', 'FRMHM':  'FRM',  'HRSHM':  'HRH', 'DORKING':'DKG',
  'EPSOM':  'EPS', 'SURBITN':'SUR',  'WEYBRIJ':'WYB', 'WALTON': 'WAL',
  'ESHER':  'ESH', 'FLEET':  'FLE',  'FARNBRM':'FNB', 'ALDRSHT':'AHT',
  'FARNHAM':'FNH', 'WIMBLDON':'WIM', 'RICHMND':'RMD', 'PUTNEY': 'PUT',
  'STAINES':'SNS', 'EGHAM':  'EGH',  'VRGNWTR':'VIR', 'WSTBYFT':'WBY',
  'ASCT':   'ACT', 'SNNGDL': 'SNG',  'CHRTSEY':'CHY', 'ADDLSTN':'ADD',
  'BYFLTNH':'BFN', 'WBYFLLT':'WBY',  'BRKWOOD':'BWD',
  // Brighton mainline
  'GATWICK':'GTW', 'REDHILL':'RDH',  'REIGATE':'REI', 'CROYDN': 'ECR',
  'BRIGHTON':'BTN','HOVE':   'HOV',  'WORTHNG':'WRH', 'EASTBRN':'EBN',
  'HAYWARD':'HYS', 'HORSHAM':'HRH',
  // Reading / West
  'READING':'RDG', 'SWINDON':'SWI',  'DIDCOT': 'DID', 'OXFORD': 'OXF',
  'BATHSPA':'BTH', 'BRISTLTM':'BRI', 'CHPNHM': 'CPM', 'NEWBURY':'NBY',
  // Midlands / North
  'BHMNSTH':'BHM', 'MNCRIAP':'MAN',  'LVRPLSH':'LIV', 'YORK':   'YRK',
  'EDINBUR':'EDB', 'GLCNTRL':'GLC',
};

function tiploc2crs(tpl) {
  return tpl ? TIPLOC_TO_CRS[tpl.toUpperCase()] || null : null;
}

const CRS_NAMES = {
  WAT:'London Waterloo', VIC:'London Victoria', LBG:'London Bridge',
  CLJ:'Clapham Junction', VXH:'Vauxhall', WIM:'Wimbledon', SUR:'Surbiton',
  WOK:'Woking', GLD:'Guildford', BSK:'Basingstoke', WIN:'Winchester',
  SOU:'Southampton Central', GTW:'Gatwick Airport', RDH:'Redhill',
  BTN:'Brighton', RDG:'Reading', OXF:'Oxford', PAD:'London Paddington',
  EUS:'London Euston', KGX:"London King's Cross", STP:'London St Pancras',
};
const crsName = crs => CRS_NAMES[crs] || crs;

// ── In-memory state ───────────────────────────────────
const departureStore  = new Map();  // "GLD_WAT" → {trains, generatedAt, disruption}
const timetableStore  = new Map();  // rid → [{tpl, crs, ptd, pta}]
const forecastStore   = new Map();  // rid → {tpl → {std, etd, sta, eta, platform, cancelled}}

let connected    = false;
let messageCount = 0;
let lastMsgAt    = null;
let startedAt    = new Date();

// ── XML parsing helpers ───────────────────────────────
const find    = (xml, re)  => { const m = xml.match(re); return m ? m[1] : null; };
const findAll = (xml, re)  => [...xml.matchAll(re)].map(m => m[1]);
const timeToMins = t => {
  if (!t || t === '—') return 9999;
  return parseInt(t.slice(0,2)) * 60 + parseInt(t.slice(3,5));
};
const minsToTime = m => {
  const h = Math.floor(m / 60) % 24;
  const mn = m % 60;
  return `${String(h).padStart(2,'0')}:${String(mn).padStart(2,'0')}`;
};

// ── Push Port XML parser ──────────────────────────────
function parsePushPort(xml) {

  // ── 1. Schedule (SC) messages → build timetable ──
  for (const sc of (xml.match(/<SC\s[^>]*>[\s\S]*?<\/SC>/g) || [])) {
    const rid = find(sc, /rid="([^"]+)"/);
    if (!rid) continue;

    const stops = [];
    for (const s of (sc.match(/<(?:OR|IP|DT|OPOR|OPIP|OPDT)\s[^>]*/g) || [])) {
      const tpl = find(s, /tpl="([^"]+)"/);
      if (!tpl) continue;
      stops.push({
        tpl,
        crs: tiploc2crs(tpl),
        ptd: find(s, /ptd="([^"]+)"/),
        pta: find(s, /pta="([^"]+)"/),
      });
    }
    if (stops.length > 0) timetableStore.set(rid, stops);
  }

  // ── 2. Forecast (TS) messages → update live times ──
  for (const ts of (xml.match(/<TS\s[^>]*>[\s\S]*?<\/TS>/g) || [])) {
    const rid = find(ts, /rid="([^"]+)"/);
    if (!rid) continue;

    if (!forecastStore.has(rid)) forecastStore.set(rid, {});
    const svcForecasts = forecastStore.get(rid);

    for (const loc of (ts.match(/<(?:fc:)?Location\s[^>]*>[\s\S]*?<\/(?:fc:)?Location>/g) || [])) {
      const tpl = find(loc, /tpl="([^"]+)"/);
      if (!tpl) continue;

      svcForecasts[tpl] = {
        std:       find(loc, /ptd="([^"]+)"/) || find(loc, /wtd="([^"]+)"/),
        etd:       find(loc, /<(?:fc:)?dep[^>]*et="([^"]+)"/) ||
                   find(loc, /<(?:fc:)?dep[^>]*at="([^"]+)"/),
        sta:       find(loc, /pta="([^"]+)"/) || find(loc, /wta="([^"]+)"/),
        eta:       find(loc, /<(?:fc:)?arr[^>]*et="([^"]+)"/) ||
                   find(loc, /<(?:fc:)?arr[^>]*at="([^"]+)"/),
        platform:  find(loc, /<(?:fc:)?plat[^>]*>([^<]+)<\/(?:fc:)?plat>/),
        cancelled: /cancelled="true"/.test(loc),
      };
    }
  }

  // ── 3. Deactivation (DR) → remove old services ──
  for (const dr of (xml.match(/<DR\s[^>]*\/>/g) || [])) {
    const rid = find(dr, /rid="([^"]+)"/);
    if (rid) { timetableStore.delete(rid); forecastStore.delete(rid); }
  }
}

// ── Rebuild departure boards ──────────────────────────
function rebuildBoards() {
  const nowMins = new Date().getHours() * 60 + new Date().getMinutes();
  const crsToTiplocs = {};
  for (const [tpl, crs] of Object.entries(TIPLOC_TO_CRS)) {
    (crsToTiplocs[crs] = crsToTiplocs[crs] || []).push(tpl.toUpperCase());
  }

  for (const orig of CONFIG.watchStations) {
    for (const dest of CONFIG.watchStations) {
      if (orig === dest) continue;
      const origTpls = crsToTiplocs[orig] || [];
      const destTpls = crsToTiplocs[dest] || [];
      if (!origTpls.length || !destTpls.length) continue;

      const departures = [];

      for (const [rid, stops] of timetableStore) {
        const origIdx = stops.findIndex(s => origTpls.includes(s.tpl.toUpperCase()));
        const destIdx = stops.findIndex(s => destTpls.includes(s.tpl.toUpperCase()));
        if (origIdx === -1 || destIdx === -1 || origIdx >= destIdx) continue;

        const origStop  = stops[origIdx];
        const destStop  = stops[destIdx];
        const svcFc     = forecastStore.get(rid) || {};
        const origFc    = svcFc[origStop.tpl] || {};
        const destFc    = svcFc[destStop.tpl]  || {};

        const std = origStop.ptd || origFc.std;
        if (!std) continue;

        const depMins = timeToMins(std);
        if (depMins < nowMins - 2 || depMins > nowMins + 180) continue;

        const etd        = origFc.etd || std;
        const cancelled  = origFc.cancelled || false;
        const delayMins  = Math.max(0, timeToMins(etd) - timeToMins(std));
        const sta        = destStop.pta || destFc.eta || '—';

        const callingPoints = stops
          .slice(origIdx + 1, destIdx + 1)
          .filter(s => s.crs)
          .map(s => ({ name: crsName(s.crs), st: s.ptd || s.pta || '—' }));

        departures.push({
          id:          rid,
          std,
          etd:         cancelled ? 'Cancelled' : etd,
          sta,
          platform:    origFc.platform || '—',
          operator:    'National Rail',
          journeyMins: sta !== '—' ? timeToMins(sta) - timeToMins(std) : null,
          status:      cancelled ? 'Cancelled' : delayMins > 0 ? `Delayed ${delayMins} minutes` : 'On time',
          isCancelled: cancelled,
          delayMins,
          callingPoints,
        });
      }

      departures.sort((a, b) => a.std.localeCompare(b.std));
      departureStore.set(`${orig}_${dest}`, {
        trains:      departures.slice(0, 5),
        generatedAt: new Date().toISOString(),
        disruption:  null,
      });
    }
  }
}

// ── Kafka consumer ────────────────────────────────────
const kafka = new Kafka({
  clientId: 'traindash-bridge',
  brokers:  [CONFIG.kafka.broker],
  ssl:      true,
  sasl: {
    mechanism: 'plain',
    username:  CONFIG.kafka.username,
    password:  CONFIG.kafka.password,
  },
  retry: { initialRetryTime: 2000, retries: 10 },
  logLevel: 1, // ERROR only — keeps Railway logs clean
});

const consumer = kafka.consumer({ groupId: CONFIG.kafka.consumerGroup });

async function startKafka() {
  console.log('🔌 Connecting to Darwin Kafka feed...');
  await consumer.connect();
  connected = true;
  console.log('✅ Connected — waiting for train data...');

  await consumer.subscribe({ topic: CONFIG.kafka.topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        const raw     = Buffer.from(payload.bytes || '', 'base64');

        let xml;
        try   { xml = zlib.gunzipSync(raw).toString('utf-8'); }
        catch { xml = raw.toString('utf-8'); }

        // Quick relevance filter — only parse if a watched TIPLOC appears
        const isRelevant = Object.keys(TIPLOC_TO_CRS)
          .some(t => CONFIG.watchStations.includes(TIPLOC_TO_CRS[t]) && xml.includes(t));

        if (!isRelevant) return;

        parsePushPort(xml);
        rebuildBoards();

        messageCount++;
        lastMsgAt = new Date();

        if (messageCount % 500 === 0) {
          console.log(`📨 ${messageCount} msgs processed | ${timetableStore.size} services tracked`);
        }

      } catch { /* skip malformed messages */ }
    },
  });
}

// ── Express REST API ──────────────────────────────────
const app = express();
app.use(cors());

// Departure board — what TrainDash calls
app.get('/departures/:from/to/:to', (req, res) => {
  const key  = `${req.params.from.toUpperCase()}_${req.params.to.toUpperCase()}`;
  const data = departureStore.get(key) || {
    trains: [], generatedAt: new Date().toISOString(), disruption: null,
  };
  res.json(data);
});

// Health check — Railway uses this to verify the app is running
app.get('/health', (req, res) => {
  res.json({
    status:          connected ? 'connected' : 'connecting',
    uptime:          Math.floor((Date.now() - startedAt) / 1000) + 's',
    messagesTotal:   messageCount,
    lastMessage:     lastMsgAt,
    servicesTracked: timetableStore.size,
    boardsBuilt:     departureStore.size,
    watchStations:   CONFIG.watchStations,
  });
});

// Root — confirms the service is alive
app.get('/', (req, res) => {
  res.json({ service: 'TrainDash Bridge', status: 'running', docs: '/health' });
});

app.listen(CONFIG.port, () => {
  console.log(`🚂 TrainDash Bridge listening on port ${CONFIG.port}`);
  console.log(`   Watching stations: ${CONFIG.watchStations.join(', ')}`);
});

// ── Graceful shutdown ─────────────────────────────────
const shutdown = async () => {
  console.log('⏹ Shutting down gracefully...');
  await consumer.disconnect();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

// ── Boot ──────────────────────────────────────────────
startKafka().catch(err => {
  console.error('❌ Failed to connect to Kafka:', err.message);
  process.exit(1);
});
