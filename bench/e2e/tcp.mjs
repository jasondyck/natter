// ICE over TCP only, run by tcp.sh, whose network drops UDP to and from the
// media ports, as one that blocks it. Three rooms (alice publishes, bob listens); with NATTER_SHARDS=3
// NATTER_SHARD_FILL=1 each room is on its own engine, all behind one TCP
// port. Logs, per room, what bob's connection runs over, to which server
// port, and whether he hears alice.
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
    window.pcs = [];
    await r.connect(url, tok);
  }, [LK, token(name, room)]);
  return p;
}
const out = [];
const pages = [];
for (let i = 0; i < 3; i++) {
  const room = `tcp${i}`;
  const alice = await join('alice', room);
  await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(true));
  const bob = await join('bob', room);
  pages.push([room, alice, bob]);
}
await sleep(4000);
for (const [room, alice, bob] of pages) {
  const got = await bob.evaluate(async () => {
    const L = LivekitClient;
    const e = window.r.engine;
    const pc = (e.pcManager.subscriber ?? e.pcManager.publisher).pc;
    const stats = [...(await pc.getStats()).values()];
    const pair = stats.find((s) => s.type === 'candidate-pair' && s.nominated && s.state === 'succeeded');
    const local = pair && stats.find((s) => s.id === pair.localCandidateId);
    const remote = pair && stats.find((s) => s.id === pair.remoteCandidateId);
    const audio = stats.filter((s) => s.type === 'inbound-rtp' && s.kind === 'audio').reduce((n, s) => n + s.bytesReceived, 0);
    return { proto: local?.protocol ?? 'none', port: remote?.port, audio };
  });
  out.push(`${room}: over ${got.proto} to port ${got.port}, ${got.audio > 1000 ? 'hears alice' : `does not hear alice (${got.audio} bytes)`}`);
  await bob.evaluate(() => window.r.disconnect());
  await alice.evaluate(() => window.r.disconnect());
}
console.log(out.join('\n'));
await browser.close();
process.exit(0);
