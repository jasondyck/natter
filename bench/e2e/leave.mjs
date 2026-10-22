// How soon the others hear someone left: alice and bob publish, alice
// disconnects, bob logs ms until ParticipantDisconnected and until he loses
// her track, five times.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
function token(identity, room) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video: { room, roomJoin: true, canPublish: true, canSubscribe: true } }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const browser = await chromium.launch({
  executablePath: process.env.CHROME, headless: true,
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'],
});
const ctx = await browser.newContext();
async function join(name, room) {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    const at = () => performance.timeOrigin + performance.now();
    r.on(L.RoomEvent.ParticipantDisconnected, () => { window.left = at(); });
    r.on(L.RoomEvent.TrackUnsubscribed, () => { window.unsub = at(); });
    await r.connect(url, tok);
    await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, token(name, room)]);
  return p;
}
const out = [];
for (let i = 0; i < 5; i++) {
  const room = `leave${i}`;
  const alice = await join('alice', room);
  const bob = await join('bob', room);
  await sleep(2000);
  const t0 = await alice.evaluate(async () => { const t = performance.timeOrigin + performance.now(); await window.r.disconnect(); return t; });
  await sleep(1500);
  const [left, unsub] = await bob.evaluate(() => [window.left, window.unsub]);
  out.push(`${left ? Math.round(left - t0) : 'never'}/${unsub ? Math.round(unsub - t0) : 'never'}`);
  await bob.evaluate(() => window.r.disconnect());
  await alice.close();
  await bob.close();
}
console.log(`ms until bob saw alice leave / lost her track: ${out.join('  ')}`);
await browser.close();
process.exit(0);
