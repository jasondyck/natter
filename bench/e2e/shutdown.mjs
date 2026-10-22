// Stopping the server mid-call (CONFIG=shutdown.yaml): alice publishes, bob
// subscribes, and an empty room waits for its timeout; then SIGTERM. Logs
// why each client was disconnected and the webhooks after the signal.
// LiveKit's first SIGTERM waits for participants to leave, so it gets a
// second one (its forced stop); natter stops on the first.
import { execSync } from 'child_process';
import { DUAL, LK, PAGE, launch, sleep, token, twirp, webhooks } from './lib.mjs';
let hooks = [];
let after = false;
webhooks((e) => {
  if (after) hooks.push([e.event, e.room?.name, e.participant?.identity ?? '', e.roomEndReason ?? ''].join(' ').trim());
});
const browser = await launch();
const ctx = await browser.newContext();
async function join(name, publish) {
  const p = await ctx.newPage();
  await p.goto(PAGE);
  if (process.env.DUAL) await p.evaluate(() => { window.DUAL = true; });
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.events = [];
    r.on(L.RoomEvent.Disconnected, (reason) => window.events.push(`disconnected ${L.DisconnectReason[reason]}`));
    r.on(L.RoomEvent.Reconnecting, () => window.events.push('reconnecting'));
    await r.connect(url, tok);
    if (publish) await r.localParticipant.setMicrophoneEnabled(true);
  }, [LK, token(name, { room: 'call', roomJoin: true, canPublish: true, canSubscribe: true }), publish]);
  return p;
}
const alice = await join('alice', true);
const bob = await join('bob', false);
// An empty room, waiting for its empty timeout when the server stops.
await twirp('CreateRoom', { name: 'empty', empty_timeout: 300 });
await sleep(3000);
after = true;
const term = () => (process.env.SERVER === 'lk' ? execSync('docker kill -s TERM natter-e2e-lk') : execSync('pkill -TERM -x natter-server'));
term();
await sleep(1500);
if (process.env.SERVER === 'lk') term();
await sleep(4000);
for (const [n, p] of [['alice', alice], ['bob', bob]]) console.log(`${n}: ${(await p.evaluate(() => window.events)).join('; ')}`);
console.log('webhooks:');
for (const h of hooks.sort()) console.log(`  ${h}`);
await browser.close();
process.exit(0);
