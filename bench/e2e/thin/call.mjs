// A call over a thin link, in Chrome (run by thin.sh): per room, 2 camera publishers (720p simulcast),
// 1 microphone publisher (none with NOMIC=1), 3 subscribers (adaptiveStream off, so they want
// the top layer). Publishers don't subscribe. After WARMUP s, signals the
// router (SIGUSR1) and measures subscribers' inbound stats over MEASURE s.
import { LK, PAGE, grant, launch, sleep, token as tok } from '../lib.mjs';
import fs from 'fs';
const WARMUP = +(process.env.WARMUP ?? 10), MEASURE = +(process.env.MEASURE ?? 20), ROOMS = +(process.env.ROOMS ?? 1);
const ROUTER_PID = +process.env.ROUTER_PID;
const browser = await launch();
const ctx = await browser.newContext();
async function join(name, room, role) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.error(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, role]) => {
    const L = LivekitClient;
    const r = new L.Room({ adaptiveStream: false, dynacast: true });
    window.r = r;
    r.on(L.RoomEvent.TrackSubscribed, (track) => {
      if (track.kind === 'audio') document.body.appendChild(track.attach());
    });
    await r.connect(url, tok, { autoSubscribe: role === 'sub' });
    if (role === 'cam') await r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { simulcast: true });
    if (role === 'mic') await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, tok(name, grant(room)), role]);
  return p;
}
const subs = [];
for (let k = 0; k < ROOMS; k++) {
  const room = `thin${k}`;
  await join(`cam1-${k}`, room, 'cam');
  await join(`cam2-${k}`, room, 'cam');
  if (!process.env.NOMIC) await join(`mic-${k}`, room, 'mic');
  for (let i = 0; i < 3; i++) subs.push(await join(`sub${i}-${k}`, room, 'sub'));
}
const snap = (p) => p.evaluate(async () => {
  const out = [];
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (!t.track) continue;
    for (const s of (await t.track.receiver.getStats()).values()) if (s.type === 'inbound-rtp') out.push({ who: w.identity, kind: s.kind, ...s });
  }
  return out;
});
await sleep(WARMUP * 1000);
if (ROUTER_PID) process.kill(ROUTER_PID, 'SIGUSR1');
const before = await Promise.all(subs.map(snap));
const heights = [];
for (let s = 0; s < MEASURE; s++) {
  await sleep(1000);
  for (const p of subs) for (const x of await snap(p)) if (x.kind === 'video') heights.push(x.frameHeight ?? 0);
}
const after = await Promise.all(subs.map(snap));
if (ROUTER_PID) process.kill(ROUTER_PID, 'SIGUSR2');
const sum = { audio: {}, video: {} };
const add = (o, k, v) => { o[k] = (o[k] ?? 0) + (v ?? 0); };
after.forEach((list, i) => {
  for (const a of list) {
    const b = before[i].find((x) => x.ssrc === a.ssrc) ?? {};
    const d = (k) => (a[k] ?? 0) - (b[k] ?? 0);
    const o = sum[a.kind];
    for (const k of ['packetsReceived', 'packetsLost', 'nackCount', 'pliCount', 'framesDecoded', 'framesDropped', 'freezeCount', 'totalFreezesDuration', 'concealedSamples', 'totalSamplesReceived', 'jitterBufferDelay', 'jitterBufferEmittedCount', 'retransmittedPacketsReceived']) add(o, k, d(k));
  }
});
const a = sum.audio, v = sum.video;
const hist = {};
for (const h of heights) hist[h] = (hist[h] ?? 0) + 1;
const res = {
  audio: {
    lost_pct: 100 * a.packetsLost / Math.max(1, a.packetsLost + a.packetsReceived),
    concealed_pct: 100 * a.concealedSamples / Math.max(1, a.totalSamplesReceived),
    jitter_buffer_ms: 1000 * a.jitterBufferDelay / Math.max(1, a.jitterBufferEmittedCount),
  },
  video: {
    lost_pct: 100 * v.packetsLost / Math.max(1, v.packetsLost + v.packetsReceived),
    fps_per_sub_track: v.framesDecoded / MEASURE / (subs.length * 2),
    freezes: v.freezeCount, freeze_s: +v.totalFreezesDuration?.toFixed(2),
    nacks: v.nackCount, plis: v.pliCount, rtx_received: v.retransmittedPacketsReceived,
    jitter_buffer_ms: 1000 * v.jitterBufferDelay / Math.max(1, v.jitterBufferEmittedCount),
    heights: hist,
  },
};
console.log(JSON.stringify(res));
if (process.env.OUT) fs.writeFileSync(process.env.OUT, JSON.stringify(res, null, 1));
await browser.close();
