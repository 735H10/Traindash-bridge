"""
TrainDash Bridge — Minimal diagnostic version
Strips out all XML parsing to confirm raw Kafka messages are arriving.
Check /health for message count and /raw for the last message received.
"""

import os, json, base64, gzip, threading
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from confluent_kafka import Consumer

BROKER   = os.environ.get('KAFKA_BROKER',   'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092')
TOPIC    = os.environ.get('KAFKA_TOPIC',    'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON')
GROUP_ID = os.environ.get('KAFKA_GROUP',    'SC-c8a3c6c8-2c1a-4063-9e5f-55beb9da3309')
USERNAME = os.environ.get('KAFKA_USERNAME', '')
PASSWORD = os.environ.get('KAFKA_PASSWORD', '')
PORT     = int(os.environ.get('PORT', 3000))

if not USERNAME or not PASSWORD:
    raise SystemExit('KAFKA_USERNAME and KAFKA_PASSWORD must be set.')

print(f'Broker:   {BROKER}')
print(f'Topic:    {TOPIC}')
print(f'Group:    {GROUP_ID}')
print(f'Username: {USERNAME}')
print(f'Password: {PASSWORD[:6]}...')

# ── Shared state ──────────────────────────────────────
state = {
    'connected':   False,
    'msg_count':   0,
    'last_raw':    None,   # raw bytes of last message value
    'last_at':     None,
    'last_error':  None,
    'started_at':  datetime.utcnow(),
    'partitions':  [],
}

# ── Kafka consumer thread ─────────────────────────────
def run_kafka():
    conf = {
        'bootstrap.servers':    BROKER,
        'security.protocol':    'SASL_SSL',
        'sasl.mechanism':       'PLAIN',
        'sasl.username':        USERNAME,
        'sasl.password':        PASSWORD,
        'group.id':             GROUP_ID,
        'auto.offset.reset':    'earliest',
        'enable.auto.commit':   False,
        'session.timeout.ms':   30000,
    }

    print('Connecting to Kafka...')
    consumer = Consumer(conf)

    def on_assign(c, partitions):
        state['partitions'] = [{'topic': p.topic, 'partition': p.partition, 'offset': p.offset} for p in partitions]
        print(f'Assigned partitions: {state["partitions"]}')

    consumer.subscribe([TOPIC], on_assign=on_assign)
    state['connected'] = True
    print('Subscribed — polling for messages...')

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                err = str(msg.error())
                print(f'Kafka error: {err}')
                state['last_error'] = err
                continue

            state['msg_count'] += 1
            state['last_at']    = datetime.utcnow().isoformat() + 'Z'
            raw_val             = msg.value()
            state['last_raw']   = raw_val[:500].decode('utf-8', errors='replace')

            if state['msg_count'] <= 3:
                print(f'--- Message #{state["msg_count"]} ---')
                print(f'Partition: {msg.partition()}, Offset: {msg.offset()}')
                print(f'Value preview: {raw_val[:300]}')

    except Exception as e:
        print(f'Consumer exception: {e}')
        state['last_error'] = str(e)
    finally:
        consumer.close()

t = threading.Thread(target=run_kafka, daemon=True)
t.start()

# ── Flask ─────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

@app.route('/health')
def health():
    uptime = int((datetime.utcnow() - state['started_at']).total_seconds())
    return jsonify({
        'status':      'connected' if state['connected'] else 'connecting',
        'uptime':      f'{uptime}s',
        'messages':    state['msg_count'],
        'lastMessage': state['last_at'],
        'lastError':   state['last_error'],
        'partitions':  state['partitions'],
    })

@app.route('/raw')
def raw():
    return jsonify({
        'lastRawMessage': state['last_raw'],
        'messageCount':   state['msg_count'],
    })

@app.route('/')
def root():
    return jsonify({'service': 'TrainDash Bridge Diagnostics', 'status': 'running', 'docs': '/health'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
