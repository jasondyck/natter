// Temporal layers by fps: alice publishes a camera (CODEC, default vp8;
// simulcast for VP8, SVC for VP9/AV1), bob asks for frame rates with
// setVideoFPS and reports what his decoder gets: frames a second (nearest
// whole number, over 4 s), the frame height, and freezes. NO_DD=1 keeps
// the dependency descriptor out of Chrome's SDP, so VP8 carries its
// temporal layers in PictureID/TID instead.
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const ctx = await browser.newContext();
if (process.env.NO_DD) {
  await ctx.addInitScript(() => {
    const strip = (d) => d && d.sdp ? { type: d.type, sdp: d.sdp.replace(/a=extmap:\d+ [^\r\n]*dependency-descriptor[^\r\n]*\r\n/g, '') } : d;
    const offer = RTCPeerConnection.prototype.createOffer;
    RTCPeerConnection.prototype.createOffer = async function (...a) { return strip(await offer.apply(this, a)); };
    const answer = RTCPeerConnection.prototype.createAnswer;
    RTCPeerConnection.prototype.createAnswer = async function (...a) { return strip(await answer.apply(this, a)); };
  });
}
const room = `fp${Date.now() % 100000}`;
const codec = process.env.CODEC ?? 'vp8';
async function join(name, publish) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish, codec]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { videoCodec: codec, simulcast: true });
    }
  }, [LK, token(name, grant(room)), publish, codec]);
  return p;
}
await join('alice', true);
await sleep(1000);
const bob = await join('bob', false);
await sleep(3000);
const pub = () => bob.evaluate(() => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) if (t.kind === 'video') return true;
  return false;
});
if (!(await pub())) console.log('bob sees no video');
const stats = () => bob.evaluate(async () => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind !== 'video' || !t.track) continue;
    const s = [...(await t.track.receiver.getStats()).values()].find((x) => x.type === 'inbound-rtp');
    return { frames: s?.framesDecoded ?? 0, at: performance.now(), height: s?.frameHeight ?? 0, freezes: s?.freezeCount ?? 0 };
  }
  return null;
});
const step = async (name, fps, quality) => {
  if (fps !== undefined || quality !== undefined) {
    await bob.evaluate(([fps, quality]) => {
      for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
        if (t.kind !== 'video') continue;
        if (fps !== null) t.setVideoFPS(fps);
        if (quality !== null) t.setVideoQuality(LivekitClient.VideoQuality[quality]);
      }
    }, [fps ?? null, quality ?? null]);
  }
  await sleep(2500);
  const a = await stats();
  await sleep(4000);
  const b = await stats();
  if (!a || !b) return console.log(`${name}: no stats`);
  const rate = ((b.frames - a.frames) * 1000) / (b.at - a.at);
  console.log(`${name}: ${Math.round(rate)} fps, ${b.height}p, freezes ${b.freezes - a.freezes}`);
};
await step('no limit');
for (const fps of [10, 5, 20, 1, 10]) await step(`setVideoFPS(${fps})`, fps);
// Switching spatial layers with a frame rate set.
await step('quality LOW', undefined, 'LOW');
await step('quality HIGH', undefined, 'HIGH');
await step('setVideoFPS(30)', 30);
await browser.close();
