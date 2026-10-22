// Hidden, recorder and agent participants with the JS SDK, run against
// natter or LiveKit on :7880: who sees whom, room counts (signalling and
// Twirp), TrackSubscribed on the publisher, data from a hidden sender, and
// whether a room with only dependents left closes.
import { LK, PAGE, grant as roomGrant, launch, sleep, token, twirp as roomService } from './lib.mjs';
const room = `hid${Date.now() % 100000}`;
const grant = (extra) => roomGrant(room, extra);
const twirp = (method, body) => roomService(method, body, room);
const browser = await launch();
const ctx = await browser.newContext();
const pages = {};
async function join(name, video, extra = {}, publish = null) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room();
    window.r = r;
    window.events = [];
    const ev = (...a) => window.events.push(a.join(' '));
    r.on(L.RoomEvent.ParticipantConnected, (who) => ev('connected', who.identity, `kind=${who.kind}`));
    r.on(L.RoomEvent.ParticipantDisconnected, (who) => ev('disconnected', who.identity));
    r.on(L.RoomEvent.TrackSubscribed, (t, pub, who) => ev('subscribed', who.identity, pub.kind));
    r.on(L.RoomEvent.LocalTrackSubscribed, (pub) => ev('local-track-subscribed', pub.kind));
    r.on(L.RoomEvent.RecordingStatusChanged, (rec) => ev('recording', rec));
    r.on(L.RoomEvent.DataReceived, (payload, who) => ev('data', new TextDecoder().decode(payload), `from=${who?.identity ?? 'none'}`));
    r.on(L.RoomEvent.Disconnected, (reason) => ev('disconnected-self', reason));
    await r.connect(url, tok);
    if (publish === 'av') { await r.localParticipant.setMicrophoneEnabled(true); await r.localParticipant.setCameraEnabled(true); }
    if (publish === 'a') await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, token(name, video, extra), publish]);
  pages[name] = p;
  return p;
}
const view = (p) => p.evaluate(() => {
  const r = window.r;
  const others = [...r.remoteParticipants.values()].map((w) => `${w.identity}(${w.kind}${w.permissions?.hidden ? ',hidden' : ''}${w.permissions?.recorder ? ',rec' : ''}:${w.trackPublications.size}t)`).sort().join(' ');
  return `sees [${others}] n=${r.numParticipants} pubs=${r.numPublishers} rec=${r.isRecording} self=${r.localParticipant.kind}`;
});
const report = async (step) => {
  console.log(`== ${step}`);
  for (const [n, p] of Object.entries(pages)) {
    if (p.isClosed()) continue;
    const evs = await p.evaluate(() => window.events.splice(0));
    console.log(`  ${n}: ${await view(p)}  events: ${evs.join('; ') || '-'}`);
  }
  const rooms = (await twirp('ListRooms', { names: [room] })).rooms ?? [];
  const r = rooms[0];
  console.log(`  twirp room: ${r ? `n=${r.num_participants ?? 0} pubs=${r.num_publishers ?? 0} rec=${r.active_recording ?? false}` : 'none'}`);
  const ps = (await twirp('ListParticipants', { room })).participants ?? [];
  console.log(`  twirp participants: ${ps.map((x) => `${x.identity}(${x.kind ?? 'STANDARD'}${x.permission?.hidden ? ',hidden' : ''})`).sort().join(' ') || '-'}`);
};
// Departure timeout short, so the dependents-only check is quick.
await twirp('CreateRoom', { name: room, departure_timeout: 5, empty_timeout: 5 });
await join('alice', grant(), {}, 'av');
await sleep(6000);
await report('alice publishes');
await join('rita', grant({ recorder: true, hidden: true }));
await sleep(6000);
await report('rita: hidden recorder joined');
await join('hank', grant({ hidden: true }), {}, 'a');
await sleep(6000);
await report('hank: hidden, publishes audio');
await join('bob', grant());
await join('agent1', grant({ agent: true }), { kind: 'agent' });
await sleep(6000);
await report('bob and agent1 joined');
await pages.hank.evaluate(() => window.r.localParticipant.publishData(new TextEncoder().encode('from hank'), { reliable: true }));
await pages.rita.evaluate(() => window.r.localParticipant.publishData(new TextEncoder().encode('from rita'), { reliable: true }));
await sleep(6000);
await report('hank and rita sent data');
for (const n of ['alice', 'bob', 'hank']) { await pages[n].evaluate(() => window.r.disconnect()); await pages[n].close(); }
await sleep(6000);
await report('alice, bob, hank left: rita and agent1 remain');
await sleep(10000);
await report('10 s later');
await browser.close();
