// fast_publish: dual-PC clients that may publish connect their publisher PC
// at join, so a first publish skips ICE/DTLS. alice (canPublish) and carol
// (canPublish: false) join; which of their PCs connect, and how long does
// alice's first publish take until bob receives it? Always dual-PC (the
// SDK negotiates at join in single-PC mode anyway). Run against natter or
// LiveKit on :7880.
import { LK, PAGE, grant, launch, sleep, token as tok } from './lib.mjs';
const room = `fp${Date.now() % 100000}`;
const token = (identity, canPublish) => tok(identity, grant(room, { canPublish }));
const browser = await launch();
const ctx = await browser.newContext();
const open = async (name, canPublish) => {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: false });
    window.r = r;
    r.on(L.RoomEvent.TrackSubscribed, () => { window.subscribedAt ??= performance.now(); });
    await r.connect(url, tok);
  }, [LK, token(name, canPublish)]);
  return p;
};
// The connection state of the SDK's two PCs.
const pcStates = (page) => page.evaluate(() => {
  const pub = window.r.engine.pcManager.publisher;
  const sub = window.r.engine.pcManager.subscriber;
  const state = (t) => (t ? t.getConnectionState() : 'none');
  return `publisher ${state(pub)}, subscriber ${state(sub)}`;
});
const bob = await open('bob', true);
const alice = await open('alice', true);
const carol = await open('carol', false);
await sleep(1500);
console.log(`alice (canPublish) after join: ${await pcStates(alice)}`);
console.log(`carol (canPublish: false) after join: ${await pcStates(carol)}`);

// First publish: alice's microphone, timed to her publish resolving and to
// bob's TrackSubscribed.
const t0 = await alice.evaluate(async () => {
  const start = performance.now() + performance.timeOrigin;
  await window.r.localParticipant.setMicrophoneEnabled(true);
  window.published = performance.now() + performance.timeOrigin - start;
  return start;
});
await sleep(2000);
const published = await alice.evaluate(() => window.published);
const received = await bob.evaluate((t0) => window.subscribedAt + performance.timeOrigin - t0, t0);
console.log(`alice published her microphone; bob subscribed: ${Number.isFinite(received)}`);
if (process.env.TIMES) console.log(`  publish took ${published.toFixed(0)} ms, bob subscribed after ${received.toFixed(0)} ms`);
await browser.close();
