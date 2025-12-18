import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

export const options = {
  scenarios: {
    load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '20s',
    },
  },
};

const target = __ENV.TARGET_URL || 'http://localhost:8080/publish';

const ids = new SharedArray('ids', function () {
  const arr = [];
  for (let i = 0; i < 14000; i++) arr.push(`e-${i}`);
  return arr;
});

function pickEventId() {
  if (Math.random() < 0.30) return ids[Math.floor(Math.random() * ids.length)];
  return `u-${__VU}-${__ITER}-${Math.random().toString(16).slice(2)}`;
}

export default function () {
  const batchSize = 50;
  const now = new Date().toISOString();

  const events = [];
  for (let i = 0; i < batchSize; i++) {
    events.push({
      topic: ['auth','payment','orders'][Math.floor(Math.random() * 3)],
      event_id: pickEventId(),
      timestamp: now,
      source: 'k6',
      payload: { n: __ITER, vu: __VU }
    });
  }

  const res = http.post(target, JSON.stringify(events), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  check(res, { 'status is 200/202': (r) => r.status === 200 || r.status === 202 });
  sleep(0.1);
}
