// The UDP-unstable fallback, run by unstable.sh, whose network drops a share
// of the UDP the server sends: alice publishes her microphone and camera,
// bob receives them. Every 5 s for 90 s, bob's connection: what it runs
// over (the selected candidate pair's local protocol), and the audio and
// video loss he saw in those 5 s; and when he reconnects. With the
// fallback on, the server moves him to ICE/TCP after 20 lossy seconds of
// the last 32 and asks him to resume.
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const browser = await launch();
const ctx = await browser.newContext();
const room = `us${Date.now() % 100000}`;
const start = Date.now();
const at = () => Math.round((Date.now() - start) / 1000);

async function join(name, publish) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  p.on('console', (m) => m.text().startsWith('EV ') && console.log(`t=${at()} ${name} ${m.text().slice(3)}`));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, publish]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    for (const ev of ['Reconnecting', 'Reconnected', 'SignalReconnecting', 'Disconnected']) {
      r.on(L.RoomEvent[ev], () => console.log(`EV ${ev}`));
    }
    await r.connect(url, tok);
    if (publish) {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name, grant(room)), publish]);
  return p;
}

await join('alice', true);
const bob = await join('bob', false);

// The receiving PC's selected pair protocol, and packets received/lost
// per kind so far.
const state = () => bob.evaluate(async () => {
  const pcs = [];
  const e = window.r.engine;
  for (const t of [e.pcManager?.subscriber, e.pcManager?.publisher]) if (t?.pc) pcs.push(t.pc);
  const pc = pcs.find((pc) => pc.getReceivers().some((r) => r.track?.readyState === 'live')) ?? pcs[0];
  const stats = [...(await pc.getStats()).values()];
  const pair = stats.find((s) => s.type === 'candidate-pair' && s.nominated && s.state === 'succeeded');
  const local = pair && stats.find((s) => s.id === pair.localCandidateId);
  const kinds = {};
  for (const s of stats.filter((s) => s.type === 'inbound-rtp')) {
    const k = (kinds[s.kind] ??= { received: 0, lost: 0 });
    k.received += s.packetsReceived ?? 0;
    k.lost += s.packetsLost ?? 0;
  }
  return { path: local ? `${local.protocol}${local.candidateType === 'relay' ? ' relay' : ''}` : '-', kinds };
});

let last = await state();
for (let k = 0; k < 18; k++) {
  await sleep(5000);
  const now = await state();
  const loss = ['audio', 'video'].map((kind) => {
    const a = last.kinds[kind] ?? { received: 0, lost: 0 }, b = now.kinds[kind] ?? { received: 0, lost: 0 };
    const recv = b.received - a.received, lost = Math.max(0, b.lost - a.lost);
    return recv + lost > 0 ? `${kind} ${Math.round((100 * lost) / (recv + lost))}% lost` : `${kind} -`;
  });
  console.log(`t=${at()} bob over ${now.path}, ${loss.join(', ')}`);
  last = now;
}
await browser.close();
