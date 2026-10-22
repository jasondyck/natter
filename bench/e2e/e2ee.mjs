// End-to-end encryption through the server, JS SDK, natter or LiveKit on
// :7880: alice publishes encrypted audio, video and data; bob has the key,
// carol a wrong one.
import { DUAL, LK, PAGE, grant, launch, sleep, token as tok } from './lib.mjs';
const room = `e2${Date.now() % 100000}`;
const token = (identity) => tok(identity, grant(room));
const browser = await launch();
const ctx = await browser.newContext();
const pages = {};
for (const [name, key] of [['alice', 'right key'], ['bob', 'right key'], ['carol', 'wrong key']]) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, key, name, dual]) => {
    const L = LivekitClient;
    const keyProvider = new L.ExternalE2EEKeyProvider();
    await keyProvider.setKey(key);
    const r = new L.Room({ singlePeerConnection: !dual, encryption: { keyProvider, worker: new Worker('livekit-client.e2ee.worker.js') } });
    window.r = r;
    window.got = new Set();
    window.data = [];
    r.on(L.RoomEvent.EncryptionError, (e) => window.got.add(`encryption error`));
    r.on(L.RoomEvent.ParticipantEncryptionStatusChanged, (on, who) => window.got.add(`${who?.identity} encrypted=${on}`));
    r.on(L.RoomEvent.DataReceived, (payload, who) => window.data.push(`data "${new TextDecoder().decode(payload)}" from ${who?.identity}`));
    r.registerTextStreamHandler('chat', async (reader, info) => window.data.push(`text "${await reader.readAll()}" from ${info.identity}`));
    r.on(L.RoomEvent.TrackSubscribed, (track) => { const el = track.attach(); el.muted = true; document.body.appendChild(el); });
    await r.setE2EEEnabled(true);
    await r.connect(url, tok);
    if (name === 'alice') {
      await r.localParticipant.setMicrophoneEnabled(true);
      await r.localParticipant.setCameraEnabled(true);
    }
  }, [LK, token(name), key, name, DUAL]);
  pages[name] = p;
}
await sleep(6000);
await pages.alice.evaluate(async () => {
  await window.r.localParticipant.publishData(new TextEncoder().encode('secret data'), { reliable: true });
  await window.r.localParticipant.sendText('secret text', { topic: 'chat' });
});
await sleep(4000);
for (const n of ['bob', 'carol']) {
  const s = await pages[n].evaluate(async () => {
    const out = { decoded: 0, audio: 0 };
    for (const w of window.r.remoteParticipants.values()) {
      for (const pub of w.trackPublications.values()) {
        const rep = await pub.track?.getRTCStatsReport?.();
        rep?.forEach((x) => {
          if (x.type !== 'inbound-rtp') return;
          if (x.kind === 'video') out.decoded += x.framesDecoded ?? 0;
          if (x.kind === 'audio') out.audio += x.totalSamplesReceived ?? 0;
        });
      }
    }
    return { ...out, events: [...window.got].sort(), data: window.data.sort(), encrypted: [...window.r.remoteParticipants.values()].map((w) => [...w.trackPublications.values()].map((p) => `${p.kind}:${p.isEncrypted}`).join(',')).join(' ') };
  });
  console.log(`${n}: video decoded ${s.decoded > 30 ? 'yes' : 'no'}; audio received ${s.audio > 0 ? 'yes' : 'no'}; publications ${s.encrypted}`);
  console.log(`  events: ${s.events.join('; ') || '-'}`);
  console.log(`  data: ${s.data.join('; ') || '-'}`);
}
await browser.close();
