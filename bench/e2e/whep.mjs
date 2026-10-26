// WHEP: plain RTCPeerConnection viewers (one audio, one video receive-only
// m-line each) watching JS SDK publishers. A viewer of alice
// (`?participant=alice`) gets her camera and microphone, not bob's; the
// others don't see viewers; when alice leaves the viewer's video stops,
// and when she comes back it carries on in the same m-lines. A viewer
// with no participant takes the first tracks to come. natter only
// (LiveKit has no WHEP): run.sh natter whep.mjs out.txt.
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const room = `wp${Date.now() % 100000}`;
const WHEP = `${LK.replace(/^ws/, 'http')}/whep/v1`;

async function join(name, publish = true) {
  const p = await (await browser.newContext()).newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room();
    window.r = r;
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { simulcast: true });
    }
  }, [LK, token(name, grant(room)), publish]);
  return p;
}

async function viewer(name, query, extra = {}) {
  const p = await (await browser.newContext()).newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  const status = await p.evaluate(async ([url, tok]) => {
    const pc = new RTCPeerConnection();
    window.pc = pc;
    pc.addTransceiver('audio', { direction: 'recvonly' });
    pc.addTransceiver('video', { direction: 'recvonly' });
    await pc.setLocalDescription(await pc.createOffer());
    await new Promise((ok) => {
      if (pc.iceGatheringState === 'complete') return ok();
      pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && ok();
    });
    const resp = await fetch(url, {
      method: 'POST', body: pc.localDescription.sdp,
      headers: { 'Content-Type': 'application/sdp', Authorization: `Bearer ${tok}` },
    });
    window.location_ = resp.headers.get('Location');
    if (resp.status !== 201) return `${resp.status} ${await resp.text()}`;
    await pc.setRemoteDescription({ type: 'answer', sdp: await resp.text() });
    return resp.status;
  }, [`${WHEP}${query}`, token(name, grant(room, extra))]);
  console.log(`${name}: POST ${status}`);
  return p;
}

const stats = (p) => p.evaluate(async () => {
  const out = { at: performance.now() };
  for (const s of (await window.pc.getStats()).values()) {
    if (s.type !== 'inbound-rtp') continue;
    if (s.kind === 'video') Object.assign(out, { frames: s.framesDecoded ?? 0, height: s.frameHeight ?? 0, ssrc: s.ssrc });
    else Object.assign(out, { audio: s.packetsReceived ?? 0, energy: s.totalAudioEnergy ?? 0 });
  }
  return out;
});
// What a viewer decoded over 3 s: whether video moved (5+ frames a
// second; the fake camera's rate varies with load), its height, and
// whether audio arrived.
async function watch(label, p) {
  const a = await stats(p);
  await sleep(3000);
  const b = await stats(p);
  const fps = (((b.frames ?? 0) - (a.frames ?? 0)) * 1000) / (b.at - a.at);
  const audio = (b.audio ?? 0) > (a.audio ?? 0) ? 'audio' : 'no audio';
  const video = fps >= 5 ? `video ${b.height}p` : 'no video';
  console.log(`${label}: ${video}, ${audio}`);
}
const others = (p) => p.evaluate(() => [...window.r.remoteParticipants.values()].map((x) => x.identity).sort().join(','));

let alice = await join('alice');
const bob = await join('bob');
const v1 = await viewer('v1', '?participant=alice');
await sleep(3000);
await watch('v1 watching alice', v1);
const vAny = await viewer('v2', '');
await sleep(3000);
await watch('v2 watching anyone', vAny);
console.log(`bob sees: ${await others(bob)}`);

// v1's video comes from alice: bob turning his camera off changes nothing.
await bob.evaluate(() => window.r.localParticipant.setCameraEnabled(false));
await sleep(1000);
await watch('v1 after bob camera off', v1);

await alice.evaluate(() => window.r.disconnect());
await alice.close();
await sleep(2000);
await watch('v1 after alice left', v1);
alice = await join('alice');
await sleep(4000);
await watch('v1 after alice rejoined', v1);

// DELETE ends the session.
const del = await v1.evaluate(async ([url, tok]) => {
  const r = await fetch(new URL(window.location_, url), { method: 'DELETE', headers: { Authorization: `Bearer ${tok}` } });
  return r.status;
}, [WHEP, token('v1', grant(room))]);
console.log(`v1: DELETE ${del}`);
await sleep(1000);
await watch('v1 after DELETE', v1);

// No subscribe grant: refused.
await viewer('v3', '?participant=alice', { canSubscribe: false });
await browser.close();
