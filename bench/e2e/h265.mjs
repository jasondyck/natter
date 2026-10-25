// H.265: a WHIP publisher (ffmpeg's libx265 through natter-whip, a
// keyframe every 2 s) and two JS SDK subscribers decoding it in hardware.
// bob is there from the start; carol joins late, so she needs a keyframe
// before her first frame; bob disables the track and enables it again.
// Run by ./h265.sh (Chrome has no software H.265 decoder and its GPU
// process can't decode it headless) instead of run.sh.
import { spawn } from 'child_process';
import { LK, PAGE, grant, launch, sleep, token } from './lib.mjs';
const room = `h5${Date.now() % 100000}`;
const repo = new URL('../..', import.meta.url).pathname;
const pub = spawn('sh', ['-c',
  'ffmpeg -hide_banner -loglevel error -re -f lavfi -i testsrc2=size=1280x720:rate=30 -t 40 ' +
  '-c:v libx265 -preset ultrafast -tune zerolatency -x265-params bframes=0:keyint=60:log-level=error ' +
  '-bsf:v hevc_metadata=aud=insert -f hevc - | "$0" --codec h265 "$1"',
  `${repo}/target/release/natter-whip`, `${LK.replace(/^ws/, 'http')}/whip/v1`],
  { env: { ...process.env, TOKEN: token('camera', grant(room)) }, stdio: ['ignore', 'ignore', 'pipe'] });
let pubLog = '';
pub.stderr.on('data', (d) => { pubLog += d; });

const browser = await launch(['--enable-features=AcceleratedVideoDecodeLinuxGL,VaapiIgnoreDriverChecks,WebRtcAllowH265Receive', '--ignore-gpu-blocklist', '--ozone-platform=wayland']);
async function join(name) {
  const p = await (await browser.newContext()).newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok]) => {
    const r = new LivekitClient.Room({ singlePeerConnection: !window.DUAL });
    window.r = r;
    window.firstFrame = null;
    r.on(LivekitClient.RoomEvent.TrackSubscribed, (t) => {
      if (t.kind !== 'video') return;
      const v = t.attach();
      document.body.append(v);
      v.requestVideoFrameCallback(() => { window.firstFrame = performance.now(); });
    });
    await r.connect(url, tok);
    window.joined = performance.now();
  }, [LK, token(name, grant(room))]);
  return p;
}
const stats = (p) => p.evaluate(async () => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) {
    if (t.kind !== 'video' || !t.track) continue;
    const all = [...(await t.track.receiver.getStats()).values()];
    const s = all.find((x) => x.type === 'inbound-rtp');
    const c = all.find((x) => x.type === 'codec' && x.id === s?.codecId);
    return { mime: t.mimeType, codec: c?.mimeType, frames: s?.framesDecoded ?? 0, at: performance.now(), height: s?.frameHeight ?? 0, lost: s?.packetsLost ?? 0 };
  }
  return null;
});
// Decoded frames a second over 3 s, to the nearest 5.
async function rate(name, p) {
  const a = await stats(p);
  await sleep(3000);
  const b = await stats(p);
  if (!a || !b) return console.log(`${name}: no video`);
  const fps = ((b.frames - a.frames) * 1000) / (b.at - a.at);
  console.log(`${name}: ${b.mime} (${b.codec}), ${Math.round(fps / 5) * 5} fps, ${b.height}p, lost ${b.lost}`);
}
const firstFrame = (p) => p.evaluate(() => window.firstFrame && Math.ceil((window.firstFrame - window.joined) / 1000));

const bob = await join('bob');
await sleep(4000);
await rate('bob', bob);
const carol = await join('carol');
await sleep(4000);
const s = await firstFrame(carol);
console.log(`carol: first frame ${s === null ? 'never' : s <= 3 ? 'within 3 s of joining' : `after ${s} s`}`);
await rate('carol', carol);
const enable = (p, on) => p.evaluate((on) => {
  for (const w of window.r.remoteParticipants.values()) for (const t of w.trackPublications.values()) if (t.kind === 'video') t.setEnabled(on);
}, on);
await enable(bob, false);
await sleep(1500);
await rate('bob disabled', bob);
await enable(bob, true);
await sleep(3000);
await rate('bob enabled again', bob);
await browser.close();
pub.kill();
if (!/connected after/.test(pubLog)) console.log('publisher:', pubLog.trim());
