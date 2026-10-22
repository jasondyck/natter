// How soon the others hear someone left: alice and bob publish, alice
// disconnects, bob logs ms until ParticipantDisconnected and until he loses
// her track, five times.
import { DUAL, LK, PAGE, grant, launch, sleep, token as tok } from './lib.mjs';
const token = (identity, room) => tok(identity, grant(room));
const browser = await launch();
const ctx = await browser.newContext();
async function join(name, room) {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    const at = () => performance.timeOrigin + performance.now();
    r.on(L.RoomEvent.ParticipantDisconnected, () => { window.left = at(); });
    r.on(L.RoomEvent.TrackUnsubscribed, () => { window.unsub = at(); });
    await r.connect(url, tok);
    await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, token(name, room)]);
  return p;
}
const out = [];
for (let i = 0; i < 5; i++) {
  const room = `leave${i}`;
  const alice = await join('alice', room);
  const bob = await join('bob', room);
  await sleep(2000);
  const t0 = await alice.evaluate(async () => { const t = performance.timeOrigin + performance.now(); await window.r.disconnect(); return t; });
  await sleep(1500);
  const [left, unsub] = await bob.evaluate(() => [window.left, window.unsub]);
  out.push(`${left ? Math.round(left - t0) : 'never'}/${unsub ? Math.round(unsub - t0) : 'never'}`);
  await bob.evaluate(() => window.r.disconnect());
  await alice.close();
  await bob.close();
}
console.log(`ms until bob saw alice leave / lost her track: ${out.join('  ')}`);
await browser.close();
process.exit(0);
