/**
 * TrainDash Bridge — Darwin Kafka → REST API
 * Uses kafkajs with correct Confluent Cloud SSL + SASL config
 */

const { Kafka, logLevel } = require('kafkajs');
const express = require('express');
const cors    = require('cors');
const zlib    = require('zlib');

// ── Config from Railway environment variables ─────────
const CONFIG = {
  kafka: {
    broker:        process.env.KAFKA_BROKER   || 'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092',
    topic:         process.env.KAFKA_TOPIC    || 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON',
    consumerGroup: process.env.KAFKA_GROUP    || 'SC-c8a3c6c8-2c1a-4063-9e5f-55beb9da3309',
    username:      process.env.KAFKA_USERNAME || '',
    password:      process.env.KAFKA_PASSWORD || '',
  },
  port:          parseInt(process.env.PORT) || 3000,
  watchStations: (process.env.WATCH_STATIONS || 'GLD,WOK,WAT,LBG,CLJ,VIC').split(','),
};

if (!CONFIG.kafka.username || !CONFIG.kafka.password) {
  console.error('❌ KAFKA_USERNAME and KAFKA_PASSWORD must be set in Railway Variables.');
  process.exit(1);
}

// Log first 6 chars of password so we can verify Railway injected it correctly
// without exposing the full secret
console.log(`🔑 Username: ${CONFIG.kafka.username}`);
console.log(`🔑 Password prefix: ${CONFIG.kafka.password.slice(0, 6)}...`);

// ── TIPLOC → CRS mapping ──────────────────────────────
const TIPLOC_TO_CRS = {
  'WATRLMN':'WAT', 'VAUXHAL':'VXH',  'CLPHMJN':'CLJ', 'VICTRIA':'VIC',
  'PADTON': 'PAD', 'KNGX':   'KGX',  'EUSTON': 'EUS', 'STPX':   'STP',
  'STPXBOX':'STP', 'LIVST':  'LST',  'CANNON': 'CST', 'LNDNBDG':'LBG',
  'CHARING':'CHX', 'BLKFRAS':'BFR',  'MRYBONE':'MYB',
  'GUILDFD':'GLD', 'WOKING': 'WOK',  'BSINGSTK':'BSK','WINCHTR':'WIN',
  'SOTON':  'SOU', 'FRMHM':  'FRM',  'HRSHM':  'HRH', 'DORKING':'DKG',
  'EPSOM':  'EPS', 'SURBITN':'SUR',  'WEYBRIJ':'WYB', 'WALTON': 'WAL',
  'ESHER':  'ESH', 'FLEET':  'FLE',  'FARNBRM':'FNB', 'ALDRSHT':'AHT',
  'FARNHAM':'FNH', 'WIMBLDON':'WIM', 'RICHMND':'RMD', 'PUTNEY': 'PUT',
  'STAINES':'SNS', 'EGHAM':  'EGH',  'VRGNWTR':'VIR', 'WSTBYFT':'WBY',
  'ASCT':   'ACT', 'SNNGDL': 'SNG',  'CHRTSEY':'CHY', 'ADDLSTN':'ADD',
  'BYFLTNH':'BFN', 'BRKWOOD':'BWD',
  'GATWICK':'GTW', 'REDHILL':'RDH',  'REIGATE':'REI',
  'BRIGHTON':'BTN','HOVE':   'HOV',  'WORTHNG':'WRH',
  'READING':'RDG', 'SWINDON':'SWI',  'DIDCOT': 'DID', 'OXFORD': 'OXF',
  'BATHSPA':'BTH', 'BRISTLTM':'BRI', 'NEWBURY':'NBY',
  'BHMNSTH':'BHM', 'MNCRIAP':'MAN',  'LVRPLSH':'LIV', 'YORK':   'YRK',
  'EDINBUR':'EDB', 'GLCNTRL':'GLC',
};
const CRS_NAMES = {
  WAT:'London Waterloo', VIC:'London Victoria', LBG:'London Bridge',
  CLJ:'Clapham Junction', VXH:'Vauxhall',       WIM:'Wimbledon',
  SUR:'Surbiton',        WOK:'Woking',          GLD:'Guildford',
  BSK:'Basingstoke',     WIN:'Winchester',      SOU:'Southampton Central',
  GTW:'Gatwick Airport', RDH:'Redhill',         BTN:'Brighton',
  RDG:'Reading',         OXF:'Oxford',          PAD:'London Paddington',
  EUS:'London Euston',   KGX:"London King's Cross",
};
const tiploc2crs = t => t ? TIPLOC_TO_CRS[t.toUpperCase()] || null : null;
const crsName    = c => CRS_NAMES[c] || c;
const toMins     = t => { if (!t || t==='—') return 9999; return +t.slice(0,2)*60 + +t.slice(3,5); };

// ── In-memory state ───────────────────────────────────
const departureStore = new Map();
const timetableStore = new Map();
const forecastStore  = new Map();
let connected = false, messageCount = 0, lastMsgAt = null;
const startedAt = new Date();

// ── XML parser ────────────────────────────────────────
const find = (s, re) => { const m = s.match(re); return m ? m[1] : null; };

function parsePushPort(xml) {
  for (const sc of (xml.match(/<SC\s[^>]*>[\s\S]*?<\/SC>/g) || [])) {
    const rid = find(sc, /rid="([^"]+)"/);
    if (!rid) continue;
    const stops = [];
    for (const s of (sc.match(/<(?:OR|IP|DT|OPOR|OPIP|OPDT)\s[^>]*/g) || [])) {
      const tpl = find(s, /tpl="([^"]+)"/);
      if (!tpl) continue;
      stops.push({ tpl, crs: tiploc2crs(tpl), ptd: find(s, /ptd="([^"]+)"/), pta: find(s, /pta="([^"]+)"/) });
    }
    if (stops.length) timetableStore.set(rid, stops);
  }
  for (const ts of (xml.match(/<TS\s[^>]*>[\s\S]*?<\/TS>/g) || [])) {
    const rid = find(ts, /rid="([^"]+)"/);
    if (!rid) continue;
    if (!forecastStore.has(rid)) forecastStore.set(rid, {});
    const svcFc = forecastStore.get(rid);
    for (const loc of (ts.match(/<(?:fc:)?Location\s[^>]*>[\s\S]*?<\/(?:fc:)?Location>/g) || [])) {
      const tpl = find(loc, /tpl="([^"]+)"/);
      if (!tpl) continue;
      svcFc[tpl] = {
        std:       find(loc, /ptd="([^"]+)"/) || find(loc, /wtd="([^"]+)"/),
        etd:       find(loc, /<(?:fc:)?dep[^>]*et="([^"]+)"/) || find(loc, /<(?:fc:)?dep[^>]*at="([^"]+)"/),
        sta:       find(loc, /pta="([^"]+)"/) || find(loc, /wta="([^"]+)"/),
        eta:       find(loc, /<(?:fc:)?arr[^>]*et="([^"]+)"/) || find(loc, /<(?:fc:)?arr[^>]*at="([^"]+)"/),
        platform:  find(loc, /<(?:fc:)?plat[^>]*>([^<]+)<\/(?:fc:)?plat>/),
        cancelled: /cancelled="true"/.test(loc),
      };
    }
  }
  for (const dr of (xml.match(/<DR\s[^>]*\/>/g) || [])) {
    const rid = find(dr, /rid="([^"]+)"/);
    if (rid) { timetableStore.delete(rid); forecastStore.delete(rid); }
  }
}

function rebuildBoards() {
  const nowMins = new Date().getHours() * 60 + new Date().getMinutes();
  const crsToTiplocs = {};
  for (const [tpl, crs] of Object.entries(TIPLOC_TO_CRS))
    (crsToTiplocs[crs] = crsToTiplocs[crs] || []).push(tpl.toUpperCase());

  for (const orig of CONFIG.watchStations) {
    for (const dest of CONFIG.watchStations) {
      if (orig === dest) continue;
      const origTpls = crsToTiplocs[orig] || [];
      const destTpls = crsToTiplocs[dest] || [];
      if (!origTpls.length || !destTpls.length) continue;
      const deps = [];
      for (const [rid, stops] of timetableStore) {
        const oi = stops.findIndex(s => origTpls.includes(s.tpl.toUpperCase()));
        const di = stops.findIndex(s => destTpls.includes(s.tpl.toUpperCase()));
        if (oi === -1 || di === -1 || oi >= di) continue;
        const os = stops[oi], ds = stops[di];
        const svcFc = forecastStore.get(rid) || {};
        const oFc = svcFc[os.tpl] || {}, dFc = svcFc[ds.tpl] || {};
        const std = os.ptd || oFc.std;
        if (!std) continue;
        const depMins = toMins(std);
        if (depMins < nowMins - 2 || depMins > nowMins + 180) continue;
        const etd = oFc.etd || std;
        const cancelled = oFc.cancelled || false;
        const delay = Math.max(0, toMins(etd) - toMins(std));
        const sta = ds.pta || dFc.eta || '—';
        const calls = stops.slice(oi+1, di+1).filter(s => s.crs)
                           .map(s => ({ name: crsName(s.crs), st: s.ptd || s.pta || '—' }));
        deps.push({
          id: rid, std, etd: cancelled ? 'Cancelled' : etd, sta,
          platform: oFc.platform || '—', operator: 'National Rail',
          journeyMins: sta !== '—' ? toMins(sta) - toMins(std) : null,
          status: cancelled ? 'Cancelled' : delay > 0 ? `Delayed ${delay} minutes` : 'On time',
          isCancelled: cancelled, delayMins: delay, callingPoints: calls,
        });
      }
      deps.sort((a, b) => a.std.localeCompare(b.std));
      departureStore.set(`${orig}_${dest}`, {
        trains: deps.slice(0, 5), generatedAt: new Date().toISOString(), disruption: null,
      });
    }
  }
}

// ── Kafka client ──────────────────────────────────────
// ssl: {} tells kafkajs to use Node's built-in TLS with system CA certs
// This trusts Confluent's Let's Encrypt certificate correctly
const kafka = new Kafka({
  clientId:  'traindash-bridge',
  brokers:   [CONFIG.kafka.broker],
  ssl:       {},                    // ← use system CA bundle, not ssl:true
  sasl: {
    mechanism: 'plain',
    username:  CONFIG.kafka.username,
    password:  CONFIG.kafka.password,
  },
  logLevel:  logLevel.ERROR,
  retry: {
    initialRetryTime: 3000,
    retries:          5,
  },
});

const consumer = kafka.consumer({
  groupId:          CONFIG.kafka.consumerGroup,
  sessionTimeout:   30000,
  heartbeatInterval: 3000,
});

async function startKafka() {
  console.log('🔌 Connecting to Confluent Kafka...');
  console.log(`   Broker: ${CONFIG.kafka.broker}`);
  console.log(`   Group:  ${CONFIG.kafka.consumerGroup}`);

  await consumer.connect();
  connected = true;
  console.log('✅ Connected — subscribing...');

  await consumer.subscribe({ topic: CONFIG.kafka.topic, fromBeginning: false });
  console.log(`📡 Subscribed to ${CONFIG.kafka.topic}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        const raw     = Buffer.from(payload.bytes || '', 'base64');
        let xml;
        try   { xml = zlib.gunzipSync(raw).toString('utf-8'); }
        catch { xml = raw.toString('utf-8'); }

        const isRelevant = Object.keys(TIPLOC_TO_CRS)
          .some(t => CONFIG.watchStations.includes(TIPLOC_TO_CRS[t]) && xml.includes(t));
        if (!isRelevant) return;

        parsePushPort(xml);
        rebuildBoards();
        messageCount++;
        lastMsgAt = new Date();
        if (messageCount % 500 === 0)
          console.log(`📨 ${messageCount} msgs | ${timetableStore.size} services`);
      } catch { /* skip malformed */ }
    },
  });
}

// ── Express API ───────────────────────────────────────
const app = express();
app.use(cors());

app.get('/departures/:from/to/:to', (req, res) => {
  const key = `${req.params.from.toUpperCase()}_${req.params.to.toUpperCase()}`;
  res.json(departureStore.get(key) || { trains: [], generatedAt: new Date().toISOString(), disruption: null });
});

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

app.get('/', (req, res) => res.json({ service: 'TrainDash Bridge', status: 'running', docs: '/health' }));

app.listen(CONFIG.port, () => {
  console.log(`🚂 TrainDash Bridge on port ${CONFIG.port}`);
  console.log(`   Watching: ${CONFIG.watchStations.join(', ')}`);
});

const shutdown = async () => {
  console.log('⏹ Shutting down...');
  await consumer.disconnect();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

startKafka().catch(err => {
  console.error('❌ Kafka connection failed:', err.message);
  process.exit(1);
});
