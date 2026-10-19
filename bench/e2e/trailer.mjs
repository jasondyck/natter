// Packet trailers (frame metadata) with the JS SDK, against natter or
// LiveKit on :7880. alice publishes video with timestamps and frame ids in
// a trailer; bob has the frame-metadata worker (CAP_PACKET_TRAILER), carol
// hasn't, so the server should strip the trailer for her.
import { createRequire } from 'module';
import crypto from 'crypto';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const LK = 'ws://localhost:7880';
const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
const b64 = (b) => Buffer.from(b).toString('base64url');
const room = `tr${Date.now() % 100000}`;
function token(identity) {
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
const pages = {};
for (const [name, worker] of [['alice', true], ['bob', true], ['carol', false]]) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, worker, name, dual]) => {
    const L = LivekitClient;
    window.stats = { trailerFrames: 0, frames: 0, metadata: 0, rendered: 0 };
    if (!worker) {
      // Read received video frames before decoding: do they still end in
      // the trailer's magic?
      const Orig = window.RTCPeerConnection;
      window.RTCPeerConnection = function (cfg) {
        const pc = new Orig({ ...cfg, encodedInsertableStreams: true });
        const set = pc.setConfiguration.bind(pc);
        pc.setConfiguration = (c) => set({ ...c, encodedInsertableStreams: true });
        pc.addEventListener('track', (e) => {
          if (e.track.kind !== 'video') return;
          const { readable, writable } = e.receiver.createEncodedStreams();
          readable.pipeThrough(new TransformStream({
            transform(frame, c) {
              const d = new Uint8Array(frame.data);
              window.stats.frames++;
              if (d.length >= 4 && String.fromCharCode(...d.slice(-4)) === 'LKTS') window.stats.trailerFrames++;
              c.enqueue(frame);
            },
          })).pipeTo(writable);
        });
        return pc;
      };
      window.RTCPeerConnection.prototype = Orig.prototype;
    }
    const opts = { singlePeerConnection: !dual };
    if (worker) opts.frameMetadata = { worker: new Worker('livekit-client.fm.worker.js') };
    const r = new L.Room(opts);
    window.r = r;
    r.on(L.RoomEvent.TrackSubscribed, (track) => {
      if (track.kind !== 'video') return;
      const v = track.attach();
      v.muted = true;
      document.body.appendChild(v);
      const tick = (_now, meta) => {
        window.stats.rendered++;
        if (track.lookupFrameMetadata?.({ rtpTimestamp: meta.rtpTimestamp })) window.stats.metadata++;
        v.requestVideoFrameCallback(tick);
      };
      v.requestVideoFrameCallback(tick);
    });
    await r.connect(url, tok);
    if (name === 'alice') {
      await r.localParticipant.setCameraEnabled(true, undefined, { simulcast: false, frameMetadata: { timestamp: true, frameId: true } });
    }
  }, [LK, token(name), worker, name, !!process.env.DUAL]);
  pages[name] = p;
}
await sleep(8000);
const features = await pages.bob.evaluate(() => [...window.r.remoteParticipants.get('alice').trackPublications.values()].map((p) => `${p.kind}:${JSON.stringify(p.trackInfo?.packetTrailerFeatures ?? [])}`).join(' '));
console.log(`track features seen by bob: ${features}`);
for (const n of ['bob', 'carol']) {
  const s = await pages[n].evaluate(() => window.stats);
  const share = (a, b) => (b ? (a / b > 0.9 ? 'most' : a > 0 ? 'some' : 'none') : 'n/a');
  console.log(`${n}: rendered ${s.rendered > 30 ? 'yes' : 'no'}; frames with metadata: ${share(s.metadata, s.rendered)}; received frames ending in LKTS: ${share(s.trailerFrames, s.frames)}`);
}
await browser.close();
