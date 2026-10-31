// Dependency descriptors across stream changes: alice and carol publish
// cameras (CODEC, default vp8; simulcast for VP8, SVC for VP9/AV1), bob
// watches both. Each step prints what bob's decoders get per publisher:
// frames a second (over 3 s, rounded to 5), height, freezes. The steps
// move bob's descriptors to another stream: a simulcast layer switch, a
// republished camera taking over his m-line, his own full reconnect, and
// alice's (a new encoder, its frame numbers starting over). A subscriber
// sent frame numbers that go back drops the keyframe and everything after.
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const room = `dd${Date.now() % 100000}`;
const codec = process.env.CODEC ?? 'vp8';

async function join(name, publish) {
  const p = await (await browser.newContext()).newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish, codec]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.camera = () => r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { videoCodec: codec, simulcast: true });
    await r.connect(url, tok);
    if (publish) await window.camera();
  }, [LK, token(name, grant(room)), publish, codec]);
  return p;
}

const alice = await join('alice', true);
const carol = await join('carol', true);
const bob = await join('bob', false);
await sleep(4000);

const stats = () => bob.evaluate(async () => {
  const out = {};
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind !== 'video' || !t.track) continue;
    const s = [...(await t.track.receiver.getStats()).values()].find((x) => x.type === 'inbound-rtp');
    out[w.identity] = { frames: s?.framesDecoded ?? 0, at: performance.now(), height: s?.frameHeight ?? 0, freezes: s?.freezeCount ?? 0 };
  }
  return out;
});
const step = async (name) => {
  await sleep(3000);
  const a = await stats();
  await sleep(3000);
  const b = await stats();
  const line = ['alice', 'carol'].map((who) => {
    const x = a[who], y = b[who];
    if (!x || !y) return `${who} -`;
    const fps = Math.round(((y.frames - x.frames) * 1000) / (y.at - x.at) / 5) * 5;
    return `${who} ${fps} fps ${y.height}p freezes ${y.freezes - x.freezes}`;
  });
  console.log(`${name}: ${line.join(', ')}`);
};
const quality = (q) => bob.evaluate((q) => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind === 'video' && w.identity === 'alice') t.setVideoQuality(LivekitClient.VideoQuality[q]);
  }
}, q);

await step('start');
await quality('LOW');
await step('alice LOW');
await quality('HIGH');
await step('alice HIGH');
await alice.evaluate(async () => {
  await window.r.localParticipant.setCameraEnabled(false);
  for (const t of window.r.localParticipant.videoTrackPublications.values()) await window.r.localParticipant.unpublishTrack(t.track);
});
await sleep(1500);
await alice.evaluate(() => window.camera());
await step('alice republished');
await bob.evaluate(() => window.r.simulateScenario('full-reconnect'));
await sleep(3000);
await step('bob full reconnect');
await alice.evaluate(() => window.r.simulateScenario('full-reconnect'));
await sleep(3000);
await step('alice full reconnect');
await browser.close();
