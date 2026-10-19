// Reliable data to someone joining: alice sends a numbered message every
// 5 ms while bob joins. Does bob get everything sent since he joined, with
// no gaps? Run against natter or LiveKit on :7880.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
const room = `jd${Date.now() % 100000}`;
function token(identity) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video: { room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true } }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const browser = await chromium.launch({ executablePath: process.env.CHROME, headless: true });
const ctx = await browser.newContext();
const open = async (name, joinedHook) => {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, dual]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !dual });
    window.r = r;
    window.got = [];
    r.on(L.RoomEvent.DataReceived, (payload) => window.got.push(Number(new TextDecoder().decode(payload))));
    window.join = () => r.connect(url, tok);
  }, [LK, token(name), !!process.env.DUAL]);
  return p;
};
const alice = await open('alice');
await alice.evaluate(() => window.join());
const bob = await open('bob');
await sleep(500);
await alice.evaluate(() => {
  window.n = 0;
  window.timer = setInterval(() => { window.r.localParticipant.publishData(new TextEncoder().encode(String(++window.n)), { reliable: true }); }, 5);
});
await sleep(1000);
// What alice has sent when bob's join starts, and when it completes.
const atStart = await alice.evaluate(() => window.n);
await bob.evaluate(() => window.join());
const atJoined = await alice.evaluate(() => window.n);
await sleep(1500);
await alice.evaluate(() => clearInterval(window.timer));
await sleep(1000);
const got = await bob.evaluate(() => window.got);
const sorted = [...got].sort((a, b) => a - b);
let gaps = 0;
for (let i = 1; i < sorted.length; i++) if (sorted[i] !== sorted[i - 1] + 1) gaps++;
const dups = got.length - new Set(got).size;
const inOrder = got.every((v, i) => i === 0 || v > got[i - 1]);
const first = sorted[0];
console.log(`bob: gaps ${gaps}, duplicates ${dups}, in order ${inOrder}; first message ${first <= atStart + 3 ? 'from around his join' : first <= atJoined ? 'during his join' : 'after his join completed'}`);
console.log(`  (join started at #${atStart}, completed by #${atJoined}, first received #${first})`);
await browser.close();
