// Crafted data packets: what the server vouches for and relays. agent1 is
// an agent; bob and carol are not. Run against natter or LiveKit on :7880.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
const room = `raw${Date.now() % 100000}`;
function token(identity, extra = {}) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + 3600, video: { room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true, ...extra.video }, ...extra.claims }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const browser = await chromium.launch({ executablePath: process.env.CHROME, headless: true });
const ctx = await browser.newContext();
const pages = {};
for (const [name, extra] of [['agent1', { video: { agent: true }, claims: { kind: 'agent' } }], ['bob', {}], ['carol', {}]]) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok]) => {
    const L = LivekitClient;
    const r = new L.Room();
    window.r = r;
    window.got = [];
    const log = (s) => window.got.push(s);
    r.on(L.RoomEvent.DataReceived, (payload, who) => log(`data ${new TextDecoder().decode(payload)} from=${who?.identity}`));
    r.on(L.RoomEvent.ChatMessage, (m, who) => log(`chat "${m.message}" from=${who?.identity} generated=${!!m.generated}`));
    r.on(L.RoomEvent.TranscriptionReceived, (segs, who) => log(`transcription "${segs.map((s) => s.text).join(' ')}" from=${who?.identity}`));
    r.registerTextStreamHandler('t', async (reader, info) => log(`text "${await reader.readAll()}" from=${info.identity} generated=${!!reader.info.generated}`));
    // Capture the next packet the SDK sends; optionally send it on, edited.
    const engine = r.engine;
    const send = engine.sendDataPacket.bind(engine);
    window.capture = () => new Promise((resolve) => {
      engine.sendDataPacket = async (packet, kind) => { engine.sendDataPacket = send; resolve({ packet, kind }); };
    });
    window.sendRaw = (packet, kind) => send(packet, kind);
    await r.connect(url, tok);
  }, [LK, token(name, extra)]);
  pages[name] = p;
}
await sleep(2000);
const carolSid = await pages.carol.evaluate(() => window.r.localParticipant.sid);
for (const sender of ['agent1', 'bob']) {
  await pages[sender].evaluate(async (carolSid) => {
    const lp = window.r.localParticipant;
    // Chat on someone else's behalf.
    let c = window.capture();
    lp.sendChatMessage('chat for ghost');
    let { packet, kind } = await c;
    packet.participantIdentity = 'ghost';
    await window.sendRaw(packet, kind);
    // A text stream header on someone else's behalf, then its chunk and
    // trailer as the SDK sends them.
    const sent = [];
    const engine = window.r.engine;
    const orig = engine.sendDataPacket;
    engine.sendDataPacket = async (p, k) => { sent.push([p, k]); };
    await lp.sendText('text for ghost', { topic: 't' });
    engine.sendDataPacket = orig;
    for (const [p, k] of sent) { p.participantIdentity = 'ghost'; await window.sendRaw(p, k); }
    // A transcription: same shape the agents SDK sends.
    c = window.capture();
    lp.publishData(new TextEncoder().encode('probe'), { reliable: true });
    ({ packet, kind } = await c);
    const t = new packet.constructor({ kind: packet.kind, value: { case: 'transcription', value: { transcribedParticipantIdentity: lp.identity, trackId: '', segments: [{ id: 's1', text: `said by ${lp.identity}`, startTime: 0n, endTime: 0n, final: true, language: 'en' }] } } });
    await window.sendRaw(t, kind);
    // The same user packet twice (same nonce): relayed once.
    c = window.capture();
    lp.publishData(new TextEncoder().encode(`dup from ${lp.identity}`), { reliable: true });
    ({ packet, kind } = await c);
    await window.sendRaw(packet, kind);
    await window.sendRaw(packet, kind);
    // Addressed by sid only (deprecated destination_sids).
    c = window.capture();
    lp.publishData(new TextEncoder().encode(`sid-only from ${lp.identity}`), { reliable: true });
    ({ packet, kind } = await c);
    packet.value.value.destinationSids = [carolSid];
    packet.value.value.destinationIdentities = [];
    packet.destinationIdentities = [];
    await window.sendRaw(packet, kind);
  }, carolSid);
  await sleep(1500);
}
for (const [n, p] of Object.entries(pages)) {
  console.log(`== ${n}`);
  for (const g of await p.evaluate(() => window.got.splice(0))) console.log(`  ${g}`);
}
await browser.close();
