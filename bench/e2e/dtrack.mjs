// Data tracks with the JS SDK, run against natter or LiveKit on :7880.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
const room = `dt${Date.now() % 100000}`;
function token(identity) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video: { room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true } }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const browser = await chromium.launch({ executablePath: process.env.CHROME, headless: true });
const ctx = await browser.newContext();
const pages = {};
for (const name of ['alice', 'bob', 'carol']) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, dual]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !dual });
    window.r = r;
    window.got = [];
    window.frames = {};
    const log = (s) => window.got.push(s);
    r.on(L.RoomEvent.DataTrackPublished, (t) => log(`published ${t.info.name} by ${t.publisherIdentity} sid=${t.info.sid.slice(0, 4)}`));
    r.on(L.RoomEvent.DataTrackUnpublished, (sid) => log(`unpublished ${sid.slice(0, 4)}`));
    r.on(L.RoomEvent.TrackSubscriptionPermissionChanged, (pub, status, who) => log(`permission ${status}`));
    // Reads a remote track's frames into window.frames[name].
    window.read = async (publisher, name) => {
      const t = await r.remoteParticipants.get(publisher).dataTracks.getDeferred(name);
      const f = (window.frames[name] = []);
      const reader = t.subscribe({ bufferSize: 256 }).getReader();
      window.readers = [...(window.readers ?? []), reader];
      (async () => {
        try {
          for (;;) {
            const { value, done } = await reader.read();
            if (done) { log(`stream ${name} ended`); break; }
            let sum = 0;
            for (const b of value.payload) sum = (sum + b) % 65521;
            f.push(`${value.payload.byteLength}/${sum}/${value.userTimestamp ?? '-'}`);
          }
        } catch (e) { log(`stream ${name} error ${e.message}`); }
      })();
    };
    await r.connect(url, tok);
  }, [LK, token(name), !!process.env.DUAL]);
  pages[name] = p;
}
await sleep(1500);
const { alice, bob, carol } = pages;
const pubResult = await alice.evaluate(async () => {
  const out = [];
  window.track = await window.r.localParticipant.publishDataTrack({ name: 'telemetry' });
  out.push(`published sid=${window.track.info.sid.slice(0, 4)} handle=${window.track.info.pubHandle}`);
  for (const name of ['telemetry', '']) {
    try { await window.r.localParticipant.publishDataTrack({ name }); out.push(`publish "${name}": ok`); }
    catch (e) { out.push(`publish "${name}": ${e.name} ${e.reason ?? ''} ${e.message}`); }
  }
  return out;
});
console.log('== alice publish');
for (const l of pubResult) console.log(`  ${l}`);
await sleep(1000);
await bob.evaluate(() => window.read('alice', 'telemetry'));
await sleep(1500);
const push = (tag) => alice.evaluate(async (tag) => {
  const t = window.track;
  let ok = 0;
  for (let i = 0; i < 20; i++) {
    const size = i === 10 ? 60000 : 100 + i;
    const payload = new Uint8Array(size).map((_, k) => (k + i) % 256);
    try { t.tryPush({ payload, userTimestamp: BigInt(1000 + i) }); ok++; } catch (e) { }
    await new Promise((r) => setTimeout(r, 20));
  }
  return `${tag}: pushed ${ok}`;
}, tag);
console.log(`  ${await push('round 1')}`);
await sleep(1500);
// Only carol may have it now.
await alice.evaluate(() => window.r.localParticipant.setTrackSubscriptionPermissions(false, [{ participantIdentity: 'carol', allowAll: true }]));
await sleep(1000);
await carol.evaluate(() => window.read('alice', 'telemetry'));
await sleep(1500);
console.log(`  ${await push('round 2')}`);
await sleep(1500);
await alice.evaluate(() => window.track.unpublish());
await sleep(1500);
for (const [n, p] of Object.entries(pages)) {
  const { got, frames } = await p.evaluate(() => ({ got: window.got.splice(0), frames: window.frames }));
  console.log(`== ${n}`);
  for (const g of got) console.log(`  ${g}`);
  for (const [name, f] of Object.entries(frames)) console.log(`  frames ${name}: ${f.length}: ${f.join(' ')}`);
}
await browser.close();
