// Stopping the server mid-call (CONFIG=shutdown.yaml): alice publishes, bob
// subscribes, and an empty room waits for its timeout; then SIGTERM. Logs
// why each client was disconnected and the webhooks after the signal.
// LiveKit's first SIGTERM waits for participants to leave, so it gets a
// second one (its forced stop); natter stops on the first.
import { createRequire } from 'module';
import crypto from 'crypto';
import http from 'http';
import { execSync } from 'child_process';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
function token(identity, video) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let hooks = [];
let after = false;
http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => { body += c; });
  req.on('end', () => {
    const e = JSON.parse(body);
    if (after) hooks.push([e.event, e.room?.name, e.participant?.identity ?? '', e.roomEndReason ?? ''].join(' ').trim());
    res.end();
  });
}).listen(8797, '127.0.0.1');
const browser = await chromium.launch({
  executablePath: process.env.CHROME, headless: true,
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'],
});
const ctx = await browser.newContext();
async function join(name, publish) {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.events = [];
    r.on(L.RoomEvent.Disconnected, (reason) => window.events.push(`disconnected ${L.DisconnectReason[reason]}`));
    r.on(L.RoomEvent.Reconnecting, () => window.events.push('reconnecting'));
    await r.connect(url, tok);
    if (publish) await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, token(name, { room: 'call', roomJoin: true, canPublish: true, canSubscribe: true }), publish]);
  return p;
}
const alice = await join('alice', true);
const bob = await join('bob', false);
// An empty room, waiting for its empty timeout when the server stops.
const admin = token('admin', { roomCreate: true });
await fetch('http://localhost:7880/twirp/livekit.RoomService/CreateRoom', {
  method: 'POST', headers: { Authorization: `Bearer ${admin}`, 'Content-Type': 'application/json' },
  body: JSON.stringify({ name: 'empty', empty_timeout: 300 }),
});
await sleep(3000);
after = true;
const term = () => (process.env.SERVER === 'lk' ? execSync('docker kill -s TERM natter-e2e-lk') : execSync('pkill -TERM -x natter-server'));
term();
await sleep(1500);
if (process.env.SERVER === 'lk') term();
await sleep(4000);
for (const [n, p] of [['alice', alice], ['bob', bob]]) console.log(`${n}: ${(await p.evaluate(() => window.events)).join('; ')}`);
console.log('webhooks:');
for (const h of hooks.sort()) console.log(`  ${h}`);
await browser.close();
process.exit(0);
