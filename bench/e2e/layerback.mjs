// A simulcast layer that stops and comes back (stream trackers): alice
// publishes a VP8 simulcast camera (no dynacast, so only she turns layers
// on and off), bob watches it at HIGH. Alice turns her top encoding off for
// 4 s and on again; bob's decoded height is printed every second from just
// before, as "t=<s> <height>p". LiveKit's packet tracker stops the layer
// after a short cycle and takes it back only after 10 s of steady packets.
// DYNACAST=1: dynacast on, and bob instead asks for LOW for 8 s (alice
// stops her upper encodings) and HIGH again; dynacast asking for the
// layers again makes them live from their first packet.
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const room = `lb${Date.now() % 100000}`;

async function join(name, publish) {
  const p = await (await browser.newContext()).newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish, dynacast]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL, dynacast: dynacast, adaptiveStream: false });
    window.r = r;
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { videoCodec: 'vp8', simulcast: true });
    }
  }, [LK, token(name, grant(room)), publish, !!process.env.DYNACAST]);
  return p;
}

const alice = await join('alice', true);
await sleep(1000);
const bob = await join('bob', false);
await sleep(6000);

const height = () => bob.evaluate(async () => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind !== 'video' || !t.track) continue;
    const s = [...(await t.track.receiver.getStats()).values()].find((x) => x.type === 'inbound-rtp');
    return s?.frameHeight ?? 0;
  }
  return 0;
});
const top = (active) => alice.evaluate(async (active) => {
  for (const t of window.r.localParticipant.videoTrackPublications.values()) {
    const sender = t.track.sender;
    const p = sender.getParameters();
    const top = p.encodings.reduce((a, e) => ((e.scaleResolutionDownBy ?? 1) < (a.scaleResolutionDownBy ?? 1) ? e : a));
    top.active = active;
    await sender.setParameters(p);
  }
}, active);

const start = Date.now();
const at = () => Math.round((Date.now() - start) / 1000);
const quality = (q) => bob.evaluate((q) => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind === 'video') t.setVideoQuality(LivekitClient.VideoQuality[q]);
  }
}, q);
const [off, on, offFor] = process.env.DYNACAST
  ? [() => quality('LOW'), () => quality('HIGH'), 8]
  : [() => top(false), () => top(true), 4];
console.log(`t=${at()} ${await height()}p`);
await off();
console.log(process.env.DYNACAST ? 'bob asks LOW' : 'top encoding off');
for (let k = 0; k < offFor; k++) {
  await sleep(1000);
  console.log(`t=${at()} ${await height()}p`);
}
await on();
console.log(process.env.DYNACAST ? 'bob asks HIGH' : 'top encoding on');
for (let k = 0; k < 16; k++) {
  await sleep(1000);
  console.log(`t=${at()} ${await height()}p`);
}
await browser.close();
