// Mute webhooks (CONFIG=shutdown.yaml for the receiver; natter needs
// NATTER_WEBHOOK_TRACK_MUTES=1): alice publishes her microphone, mutes and
// unmutes it, then an admin mutes it (MutePublishedTrack). Logs the track
// webhooks as Stoat's voice-ingress reads them. Compare with Stoat's fork
// (LK_IMAGE=ghcr.io/stoatchat/livekit-server:v1.9.14); upstream LiveKit
// sends no track_muted or track_unmuted.
import { createRequire } from 'module';
import crypto from 'crypto';
import http from 'http';
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
const admin = token('admin', { roomAdmin: true, roomCreate: true, room: 'call' });
async function twirp(method, body) {
  const r = await fetch(`http://localhost:7880/twirp/livekit.RoomService/${method}`, {
    method: 'POST', headers: { Authorization: `Bearer ${admin}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return r.json();
}
let hooks = [];
http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => { body += c; });
  req.on('end', () => {
    const e = JSON.parse(body);
    if (e.event.startsWith('track_')) {
      const t = e.track ?? {};
      hooks.push([e.event, `room=${e.room?.name}`, `meta=${e.room?.metadata ?? ''}`, `who=${e.participant?.identity}`,
        `psid=${e.participant?.sid ? 'yes' : 'no'}`, `type=${t.type ?? 'AUDIO'}`, `source=${t.source}`, `muted=${t.muted ?? false}`].join(' '));
    }
    res.end();
  });
}).listen(8797, '127.0.0.1');
const browser = await chromium.launch({
  executablePath: process.env.CHROME, headless: true,
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required'],
});
const ctx = await browser.newContext();
await twirp('CreateRoom', { name: 'call', metadata: '{"stoat":1}' });
const alice = await ctx.newPage();
await alice.goto(PAGE);
if (process.env.DUAL) await alice.evaluate(() => { window.DUAL = true; });
await alice.evaluate(async ([url, tok]) => {
  const r = new LivekitClient.Room({ singlePeerConnection: !window.DUAL });
  window.r = r;
  await r.connect(url, tok);
  await r.localParticipant.setMicrophoneEnabled(true);
}, [LK, token('alice', { room: 'call', roomJoin: true, canPublish: true, canSubscribe: true })]);
const step = async (name) => {
  await sleep(2000);
  console.log(`${name}:`);
  for (const h of hooks.splice(0)) console.log(`  ${h}`);
};
await step('published');
await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(false));
await step('alice muted');
await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(true));
await step('alice unmuted');
const sid = await alice.evaluate(() => [...window.r.localParticipant.trackPublications.values()][0].trackSid);
await twirp('MutePublishedTrack', { room: 'call', identity: 'alice', track_sid: sid, muted: true });
await step('admin muted');
await browser.close();
process.exit(0);
