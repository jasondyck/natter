// ICE over TCP only: Chrome is told not to use UDP, as on a network that
// blocks it. Three rooms (alice publishes, bob listens); with NATTER_SHARDS=3
// NATTER_SHARD_FILL=1 each room is on its own engine, all behind one TCP
// port. Logs, per room, what bob's connection runs over, to which server
// port, and whether he hears alice.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = process.env.LK_URL ?? 'ws://localhost:7880';
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
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required',
    '--force-webrtc-ip-handling-policy=disable_non_proxied_udp'],
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
    window.pcs = [];
    await r.connect(url, tok);
  }, [LK, token(name, room)]);
  return p;
}
const out = [];
const pages = [];
for (let i = 0; i < 3; i++) {
  const room = `tcp${i}`;
  const alice = await join('alice', room);
  await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(true));
  const bob = await join('bob', room);
  pages.push([room, alice, bob]);
}
await sleep(4000);
for (const [room, alice, bob] of pages) {
  const got = await bob.evaluate(async () => {
    const L = LivekitClient;
    const e = window.r.engine;
    const pc = (e.pcManager.subscriber ?? e.pcManager.publisher).pc;
    const stats = [...(await pc.getStats()).values()];
    const pair = stats.find((s) => s.type === 'candidate-pair' && s.nominated && s.state === 'succeeded');
    const local = pair && stats.find((s) => s.id === pair.localCandidateId);
    const remote = pair && stats.find((s) => s.id === pair.remoteCandidateId);
    const audio = stats.filter((s) => s.type === 'inbound-rtp' && s.kind === 'audio').reduce((n, s) => n + s.bytesReceived, 0);
    return { proto: local?.protocol ?? 'none', port: remote?.port, audio };
  });
  out.push(`${room}: over ${got.proto} to port ${got.port}, ${got.audio > 1000 ? 'hears alice' : `does not hear alice (${got.audio} bytes)`}`);
  await bob.evaluate(() => window.r.disconnect());
  await alice.evaluate(() => window.r.disconnect());
}
console.log(out.join('\n'));
await browser.close();
process.exit(0);
