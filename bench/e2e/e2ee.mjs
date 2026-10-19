// End-to-end encryption through the server, JS SDK, natter or LiveKit on
// :7880: alice publishes encrypted audio, video and data; bob has the key,
// carol a wrong one.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
const room = `e2${Date.now() % 100000}`;
function token(identity) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video: { room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true } }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const browser = await chromium.launch({
  executablePath: process.env.CHROME, headless: true,
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'],
});
const ctx = await browser.newContext();
const pages = {};
for (const [name, key] of [['alice', 'right key'], ['bob', 'right key'], ['carol', 'wrong key']]) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, key, name, dual]) => {
    const L = LivekitClient;
    const keyProvider = new L.ExternalE2EEKeyProvider();
    await keyProvider.setKey(key);
    const r = new L.Room({ singlePeerConnection: !dual, encryption: { keyProvider, worker: new Worker('livekit-client.e2ee.worker.js') } });
    window.r = r;
    window.got = new Set();
    window.data = [];
    r.on(L.RoomEvent.EncryptionError, (e) => window.got.add(`encryption error`));
    r.on(L.RoomEvent.ParticipantEncryptionStatusChanged, (on, who) => window.got.add(`${who?.identity} encrypted=${on}`));
    r.on(L.RoomEvent.DataReceived, (payload, who) => window.data.push(`data "${new TextDecoder().decode(payload)}" from ${who?.identity}`));
    r.registerTextStreamHandler('chat', async (reader, info) => window.data.push(`text "${await reader.readAll()}" from ${info.identity}`));
    r.on(L.RoomEvent.TrackSubscribed, (track) => { const el = track.attach(); el.muted = true; document.body.appendChild(el); });
    await r.setE2EEEnabled(true);
    await r.connect(url, tok);
    if (name === 'alice') {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name), key, name, !!process.env.DUAL]);
  pages[name] = p;
}
await sleep(6000);
await pages.alice.evaluate(async () => {
  await window.r.localParticipant.publishData(new TextEncoder().encode('secret data'), { reliable: true });
  await window.r.localParticipant.sendText('secret text', { topic: 'chat' });
});
await sleep(4000);
for (const n of ['bob', 'carol']) {
  const s = await pages[n].evaluate(async () => {
    const out = { decoded: 0, audio: 0 };
    for (const w of window.r.remoteParticipants.values()) {
      for (const pub of w.trackPublications.values()) {
        const rep = await pub.track?.getRTCStatsReport?.();
        rep?.forEach((x) => {
          if (x.type !== 'inbound-rtp') return;
          if (x.kind === 'video') out.decoded += x.framesDecoded ?? 0;
          if (x.kind === 'audio') out.audio += x.totalSamplesReceived ?? 0;
        });
      }
    }
    return { ...out, events: [...window.got].sort(), data: window.data.sort(), encrypted: [...window.r.remoteParticipants.values()].map((w) => [...w.trackPublications.values()].map((p) => `${p.kind}:${p.isEncrypted}`).join(',')).join(' ') };
  });
  console.log(`${n}: video decoded ${s.decoded > 30 ? 'yes' : 'no'}; audio received ${s.audio > 0 ? 'yes' : 'no'}; publications ${s.encrypted}`);
  console.log(`  events: ${s.events.join('; ') || '-'}`);
  console.log(`  data: ${s.data.join('; ') || '-'}`);
}
await browser.close();
