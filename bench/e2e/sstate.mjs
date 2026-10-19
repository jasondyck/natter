// stream_state_update as the subscriber receives it: alice publishes camera
// and microphone, bob subscribes; then alice mutes and unmutes the camera,
// and bob disables and re-enables it. Logs each step's messages.
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
const room = `ss${Date.now() % 100000}`;
async function join(name, publish) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.events = [];
    const kind = (sid) => {
      for (const w of r.remoteParticipants.values()) for (const t of w.trackPublications.values()) if (t.trackSid === sid) return t.kind;
      return sid;
    };
    // Raw messages, from the signal client (the engine is made in connect).
    r.on(L.RoomEvent.SignalConnected, () => {
      const c = r.engine.client;
      const room = c.onStreamStateUpdate;
      c.onStreamStateUpdate = (u) => {
        for (const s of u.streamStates) window.events.push(`${kind(s.trackSid)} ${s.state === 1 ? 'PAUSED' : 'ACTIVE'}`);
        room?.(u);
      };
    });
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name, room), publish]);
  return p;
}
const alice = await join('alice', true);
await sleep(1000);
const bob = await join('bob', false);
const step = async (name) => {
  await sleep(4000);
  console.log(`${name}: ${(await bob.evaluate(() => window.events.splice(0))).join('; ') || '-'}`);
};
await step('subscribed');
await alice.evaluate(() => window.r.localParticipant.setCameraEnabled(false));
await step('camera muted');
await alice.evaluate(() => window.r.localParticipant.setCameraEnabled(true));
await step('camera unmuted');
const setVideo = (on) => bob.evaluate((on) => { for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) if (t.kind === 'video') t.setEnabled(on); }, on);
await setVideo(false);
await step('bob disabled video');
await setVideo(true);
await step('bob enabled video');
await browser.close();
