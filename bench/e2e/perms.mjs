// Subscription permissions with the JS SDK: alice publishes audio+video and
// changes who may subscribe; bob, carol (auto-subscribe) and dave (recorder)
// log what they get. Run against natter or LiveKit on :7880, same output.
import { DUAL, LK, PAGE, grant, launch, sleep, token as tok } from './lib.mjs';
const browser = await launch();
const ctx = await browser.newContext();
const room = `perm${Date.now() % 100000}`;
const token = (identity, room, extra = {}) => tok(identity, grant(room, extra));
async function join(name, extra, publish) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok, name, publish]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.events = [];
    const t0 = performance.now();
    const ev = (...a) => window.events.push(a.join(' '));
    r.on(L.RoomEvent.TrackSubscribed, (t, pub, who) => ev('subscribed', who.identity, pub.kind));
    r.on(L.RoomEvent.TrackUnsubscribed, (t, pub, who) => ev('unsubscribed', who.identity, pub.kind));
    r.on(L.RoomEvent.TrackSubscriptionPermissionChanged, (pub, status, who) => ev('permission', who.identity, pub.kind, status));
    r.on(L.RoomEvent.TrackSubscriptionFailed, (sid, who, err) => ev('failed', who?.identity, err?.message));
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name, room, extra), name, publish]);
  return p;
}
// What each subscriber has now: per remote track, subscribed and bytes in.
const state = (p) => p.evaluate(async () => {
  const out = [];
  for (const who of window.r.remoteParticipants.values()) {
    for (const pub of who.trackPublications.values()) {
      let bytes = 0;
      if (pub.track?.receiver) (await pub.track.receiver.getStats()).forEach((s) => { if (s.type === 'inbound-rtp') bytes += s.bytesReceived; });
      out.push(`${who.identity}/${pub.kind}:${pub.isSubscribed ? 'sub' : '-'}${pub.isAllowed === false ? ',denied' : ''}${bytes > 0 ? ',media' : ''}`);
    }
  }
  return out.sort().join(' ');
});
const subs = {};
const report = async (step) => {
  console.log(`== ${step}`);
  for (const [n, p] of Object.entries(subs)) {
    // Sorted: LiveKit re-checks a publisher's tracks in Go map order, so
    // the order of one step's events differs from run to run.
    const evs = (await p.evaluate(() => window.events.splice(0))).sort();
    console.log(`  ${n}: ${await state(p)}  events: ${evs.join('; ') || '-'}`);
  }
};
const alice = await join('alice', {}, true);
await sleep(1500);
const sids = await alice.evaluate(() => Object.fromEntries([...window.r.localParticipant.trackPublications.values()].map((p) => [p.kind, p.trackSid])));
// bob may have the audio only, nobody else anything, before anyone joins.
await alice.evaluate((audio) => window.r.localParticipant.setTrackSubscriptionPermissions(false, [{ participantIdentity: 'bob', allowAll: false, allowedTrackSids: [audio] }]), sids.audio);
await sleep(500);
subs.bob = await join('bob', {}, false);
subs.carol = await join('carol', {}, false);
subs.dave = await join('dave', { recorder: true }, false);
await sleep(4000);
await report('bob: audio only; carol: none; dave recorder');
await alice.evaluate(() => window.r.localParticipant.setTrackSubscriptionPermissions(true));
await sleep(4000);
await report('all allowed');
await alice.evaluate(() => window.r.localParticipant.setTrackSubscriptionPermissions(false, [{ participantIdentity: 'carol', allowAll: true }]));
await sleep(4000);
await report('carol only');
// bob unsubscribes from the video it is refused, then is allowed again:
// it should stay unsubscribed (no longer wanted).
await subs.bob.evaluate(() => { for (const w of window.r.remoteParticipants.values()) for (const p of w.trackPublications.values()) if (p.kind === 'video') p.setSubscribed(false); });
await sleep(1000);
await alice.evaluate(() => window.r.localParticipant.setTrackSubscriptionPermissions(true));
await sleep(4000);
await report('bob unsubscribed video, then all allowed');
await browser.close();
