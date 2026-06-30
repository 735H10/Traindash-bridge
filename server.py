"""
TrainDash Bridge — Darwin Kafka → REST API
Uses confluent-kafka (Python) — the officially supported client
per Rail Data Marketplace documentation RSPS5053.
"""

import os, json, base64, gzip, re, threading
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from confluent_kafka import Consumer, KafkaException

# ── Config from Railway environment variables ─────────
BROKER        = os.environ.get('KAFKA_BROKER',   'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092')
TOPIC         = os.environ.get('KAFKA_TOPIC',    'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON')
GROUP_ID      = os.environ.get('KAFKA_GROUP',    'SC-c8a3c6c8-2c1a-4063-9e5f-55beb9da3309')
USERNAME      = os.environ.get('KAFKA_USERNAME', '')
PASSWORD      = os.environ.get('KAFKA_PASSWORD', '')
WATCH_RAW     = os.environ.get('WATCH_STATIONS', 'GLD,WOK,WAT,LBG,CLJ,VIC')
WATCH         = [s.strip() for s in WATCH_RAW.split(',')]
PORT          = int(os.environ.get('PORT', 3000))

if not USERNAME or not PASSWORD:
    raise SystemExit('❌ KAFKA_USERNAME and KAFKA_PASSWORD must be set in Railway Variables.')

print(f'🔑 Username: {USERNAME}')
print(f'🔑 Password prefix: {PASSWORD[:6]}...')

# ── TIPLOC → CRS ──────────────────────────────────────
TIPLOC = {
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
}
CRS_NAMES = {
    'WAT':'London Waterloo', 'VIC':'London Victoria', 'LBG':'London Bridge',
    'CLJ':'Clapham Junction','VXH':'Vauxhall',        'WIM':'Wimbledon',
    'SUR':'Surbiton',        'WOK':'Woking',           'GLD':'Guildford',
    'BSK':'Basingstoke',     'WIN':'Winchester',       'SOU':'Southampton Central',
    'GTW':'Gatwick Airport', 'RDH':'Redhill',          'BTN':'Brighton',
    'RDG':'Reading',         'OXF':'Oxford',           'PAD':'London Paddington',
    'EUS':'London Euston',   'KGX':"London King's Cross",
}

def to_mins(t):
    if not t or t == '—': return 9999
    return int(t[:2]) * 60 + int(t[3:5])

def find(s, pattern):
    m = re.search(pattern, s)
    return m.group(1) if m else None

# ── In-memory state ───────────────────────────────────
departure_store = {}   # "GLD_WAT" → {trains, generatedAt, disruption}
timetable_store = {}   # rid → [{tpl, crs, ptd, pta}]
forecast_store  = {}   # rid → {tpl → {std, etd, sta, eta, platform, cancelled}}

connected    = False
msg_count    = 0
last_msg_at  = None
started_at   = datetime.utcnow()
lock         = threading.Lock()

# ── XML parser ────────────────────────────────────────
def parse_push_port(xml):
    # Schedule blocks → timetable
    for sc in re.findall(r'<SC\s[^>]*>[\s\S]*?</SC>', xml):
        rid = find(sc, r'rid="([^"]+)"')
        if not rid: continue
        stops = []
        for s in re.findall(r'<(?:OR|IP|DT|OPOR|OPIP|OPDT)\s[^>]*', sc):
            tpl = find(s, r'tpl="([^"]+)"')
            if not tpl: continue
            crs = TIPLOC.get(tpl.upper())
            stops.append({'tpl': tpl, 'crs': crs,
                          'ptd': find(s, r'ptd="([^"]+)"'),
                          'pta': find(s, r'pta="([^"]+)"')})
        if stops:
            timetable_store[rid] = stops

    # Forecast blocks → live times
    for ts in re.findall(r'<TS\s[^>]*>[\s\S]*?</TS>', xml):
        rid = find(ts, r'rid="([^"]+)"')
        if not rid: continue
        if rid not in forecast_store:
            forecast_store[rid] = {}
        for loc in re.findall(r'<(?:fc:)?Location\s[^>]*>[\s\S]*?</(?:fc:)?Location>', ts):
            tpl = find(loc, r'tpl="([^"]+)"')
            if not tpl: continue
            forecast_store[rid][tpl] = {
                'std':       find(loc, r'ptd="([^"]+)"') or find(loc, r'wtd="([^"]+)"'),
                'etd':       find(loc, r'<(?:fc:)?dep[^>]*et="([^"]+)"') or
                             find(loc, r'<(?:fc:)?dep[^>]*at="([^"]+)"'),
                'sta':       find(loc, r'pta="([^"]+)"') or find(loc, r'wta="([^"]+)"'),
                'eta':       find(loc, r'<(?:fc:)?arr[^>]*et="([^"]+)"') or
                             find(loc, r'<(?:fc:)?arr[^>]*at="([^"]+)"'),
                'platform':  find(loc, r'<(?:fc:)?plat[^>]*>([^<]+)</(?:fc:)?plat>'),
                'cancelled': 'cancelled="true"' in loc,
            }

    # Deactivation → clean up
    for dr in re.findall(r'<DR\s[^>]*/>', xml):
        rid = find(dr, r'rid="([^"]+)"')
        if rid:
            timetable_store.pop(rid, None)
            forecast_store.pop(rid, None)

def rebuild_boards():
    now_mins = datetime.now().hour * 60 + datetime.now().minute
    crs_to_tiplocs = {}
    for tpl, crs in TIPLOC.items():
        crs_to_tiplocs.setdefault(crs, []).append(tpl.upper())

    for orig in WATCH:
        for dest in WATCH:
            if orig == dest: continue
            orig_tpls = crs_to_tiplocs.get(orig, [])
            dest_tpls = crs_to_tiplocs.get(dest, [])
            if not orig_tpls or not dest_tpls: continue

            deps = []
            for rid, stops in list(timetable_store.items()):
                oi = next((i for i, s in enumerate(stops)
                           if s['tpl'].upper() in orig_tpls), -1)
                di = next((i for i, s in enumerate(stops)
                           if s['tpl'].upper() in dest_tpls), -1)
                if oi == -1 or di == -1 or oi >= di: continue

                os_ = stops[oi]
                ds_ = stops[di]
                svc_fc = forecast_store.get(rid, {})
                o_fc = svc_fc.get(os_['tpl'], {})
                d_fc = svc_fc.get(ds_['tpl'], {})

                std = os_.get('ptd') or o_fc.get('std')
                if not std: continue

                dep_mins = to_mins(std)
                if dep_mins < now_mins - 2 or dep_mins > now_mins + 180: continue

                etd = o_fc.get('etd') or std
                cancelled = o_fc.get('cancelled', False)
                delay = max(0, to_mins(etd) - to_mins(std))
                sta = ds_.get('pta') or d_fc.get('eta') or '—'
                calls = [
                    {'name': CRS_NAMES.get(s['crs'], s['crs']),
                     'st':   s.get('ptd') or s.get('pta') or '—'}
                    for s in stops[oi+1:di+1] if s.get('crs')
                ]
                deps.append({
                    'id': rid, 'std': std,
                    'etd': 'Cancelled' if cancelled else etd,
                    'sta': sta,
                    'platform': o_fc.get('platform') or '—',
                    'operator': 'National Rail',
                    'journeyMins': to_mins(sta) - to_mins(std) if sta != '—' else None,
                    'status': 'Cancelled' if cancelled else
                              f'Delayed {delay} minutes' if delay > 0 else 'On time',
                    'isCancelled': cancelled,
                    'delayMins': delay,
                    'callingPoints': calls,
                })

            deps.sort(key=lambda x: x['std'])
            departure_store[f'{orig}_{dest}'] = {
                'trains': deps[:5],
                'generatedAt': datetime.utcnow().isoformat() + 'Z',
                'disruption': None,
            }

# ── Kafka consumer thread ─────────────────────────────
def run_kafka():
    global connected, msg_count, last_msg_at

    # confluent-kafka config — matches official RDM documentation exactly
    conf = {
        'bootstrap.servers':  BROKER,
        'security.protocol':  'SASL_SSL',
        'sasl.mechanism':     'PLAIN',
        'sasl.username':      USERNAME,
        'sasl.password':      PASSWORD,
        'group.id':           GROUP_ID,
        'auto.offset.reset':  'latest',
        'enable.auto.commit': True,
        'session.timeout.ms': 30000,
    }

    print(f'🔌 Connecting to Kafka broker: {BROKER}')
    print(f'   Topic:  {TOPIC}')
    print(f'   Group:  {GROUP_ID}')

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    connected = True
    print('✅ Connected — consuming messages...')

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'❌ Kafka error: {msg.error()}')
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                raw = base64.b64decode(payload.get('bytes', ''))
                try:
                    xml = gzip.decompress(raw).decode('utf-8')
                except Exception:
                    xml = raw.decode('utf-8')

                # Quick relevance filter
                if not any(t in xml for t in TIPLOC if TIPLOC[t] in WATCH):
                    continue

                with lock:
                    parse_push_port(xml)
                    rebuild_boards()

                msg_count += 1
                last_msg_at = datetime.utcnow().isoformat() + 'Z'

                if msg_count % 500 == 0:
                    print(f'📨 {msg_count} msgs | {len(timetable_store)} services tracked')

            except Exception as e:
                pass  # skip malformed messages

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# ── Flask API ─────────────────────────────────────────
app = Flask(__name__)
CORS(app)

@app.route('/departures/<frm>/to/<to>')
def departures(frm, to):
    key = f'{frm.upper()}_{to.upper()}'
    with lock:
        data = departure_store.get(key, {
            'trains': [], 'generatedAt': datetime.utcnow().isoformat() + 'Z', 'disruption': None
        })
    return jsonify(data)

@app.route('/health')
def health():
    uptime = int((datetime.utcnow() - started_at).total_seconds())
    return jsonify({
        'status':          'connected' if connected else 'connecting',
        'uptime':          f'{uptime}s',
        'messagesTotal':   msg_count,
        'lastMessage':     last_msg_at,
        'servicesTracked': len(timetable_store),
        'boardsBuilt':     len(departure_store),
        'watchStations':   WATCH,
    })

@app.route('/')
def root():
    return jsonify({'service': 'TrainDash Bridge', 'status': 'running', 'docs': '/health'})

if __name__ == '__main__':
    t = threading.Thread(target=run_kafka, daemon=True)
    t.start()
    print(f'🚂 TrainDash Bridge on port {PORT}')
    print(f'   Watching: {", ".join(WATCH)}')
    app.run(host='0.0.0.0', port=PORT)
