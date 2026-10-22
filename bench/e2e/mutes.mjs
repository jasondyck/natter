// Mute webhooks (CONFIG=shutdown.yaml for the receiver; natter needs
// NATTER_WEBHOOK_TRACK_MUTES=1): alice publishes her microphone, mutes and
// unmutes it, then an admin mutes it (MutePublishedTrack). Logs the track
// webhooks as Stoat's voice-ingress reads them. Compare with Stoat's fork
// (LK_IMAGE=ghcr.io/stoatchat/livekit-server:v1.9.14); upstream LiveKit
// sends no track_muted or track_unmuted.
import { DUAL, LK, PAGE, launch, sleep, token, twirp as roomService, webhooks } from './lib.mjs';
const twirp = (method, body) => roomService(method, body, 'call');
let hooks = [];
webhooks((e) => {
  if (e.event.startsWith('track_')) {
    const t = e.track ?? {};
    hooks.push([e.event, `room=${e.room?.name}`, `meta=${e.room?.metadata ?? ''}`, `who=${e.participant?.identity}`,
      `psid=${e.participant?.sid ? 'yes' : 'no'}`, `type=${t.type ?? 'AUDIO'}`, `source=${t.source}`, `muted=${t.muted ?? false}`].join(' '));
  }
});
const browser = await launch();
const ctx = await browser.newContext();
await twirp('CreateRoom', { name: 'call', metadata: '{"stoat":1}' });
const alice = await ctx.newPage();
await alice.goto(PAGE);
if (process.env.DUAL) await alice.evaluate(() => { window.DUAL = true; });
await alice.evaluate(async ([url, tok]) => {
  const r = new LivekitClient.Room({ singlePeerConnection: !window.DUAL });
  window.r = r;
  await r.connect(url, tok);
  await r.localParticipant.setMicrophoneEnabled(true);
}, [LK, token('alice', { room: 'call', roomJoin: true, canPublish: true, canSubscribe: true })]);
const step = async (name) => {
  await sleep(2000);
  console.log(`${name}:`);
  for (const h of hooks.splice(0)) console.log(`  ${h}`);
};
await step('published');
await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(false));
await step('alice muted');
await alice.evaluate(() => window.r.localParticipant.setMicrophoneEnabled(true));
await step('alice unmuted');
const sid = await alice.evaluate(() => [...window.r.localParticipant.trackPublications.values()][0].trackSid);
await twirp('MutePublishedTrack', { room: 'call', identity: 'alice', track_sid: sid, muted: true });
await step('admin muted');
await browser.close();
process.exit(0);
