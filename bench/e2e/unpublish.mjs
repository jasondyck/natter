// Unpublishing one of several tracks: alice publishes her microphone and
// camera, then unpublishes the microphone; bob still decodes her camera
// and the server is still up. (A stale SSRC-to-track index once took the
// engine down on the camera's next packet.)
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const room = `up${Date.now() % 100000}`;

async function join(name, publish) {
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
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name, grant(room)), publish]);
  return p;
}

const frames = (p) => p.evaluate(() => {
  const t = [...window.r.remoteParticipants.values()][0]?.videoTrackPublications.values().next().value?.track;
  return t?.mediaStreamTrack ? new Promise((ok) => {
    const v = document.createElement('video');
    v.srcObject = new MediaStream([t.mediaStreamTrack]);
    v.muted = true;
    v.play();
    setTimeout(() => ok(v.getVideoPlaybackQuality().totalVideoFrames), 2000);
  }) : 0;
});

const alice = await join('alice', true);
const bob = await join('bob', false);
await sleep(2000);
console.log('before: bob decodes video', (await frames(bob)) > 10);
await alice.evaluate(async () => {
  const pub = [...window.r.localParticipant.audioTrackPublications.values()][0];
  await window.r.localParticipant.unpublishTrack(pub.track);
});
await sleep(2000);
console.log('after unpublishing the microphone: bob decodes video', (await frames(bob)) > 10);
console.log('server up', (await fetch(`${LK.replace(/^ws/, 'http')}/rtc/validate`)).status !== 0);
await browser.close();
