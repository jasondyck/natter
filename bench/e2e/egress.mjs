// Track egress: a JS SDK publisher's microphone and simulcast camera
// recorded through LiveKit's Egress API (StartTrackEgress, ListEgress,
// StopEgress), one stopped by the API and one by unpublishing. Prints the
// statuses, the webhooks in order, the room's active_recording, the
// errors, and what ffprobe makes of the files (2 s muted mid-way). natter only (LiveKit needs
// its egress service and Redis): CONFIG=shutdown.yaml run.sh natter
// egress.mjs out.txt.
import { execFileSync } from 'child_process';
import { LK, PAGE, grant, launch, sleep, token, twirp, webhooks } from './lib.mjs';

const browser = await launch();
const room = `eg${Date.now() % 100000}`;
const API = LK.replace(/^ws/, 'http');

const hooks = [];
const receiver = webhooks((e) => {
  if (e.event.startsWith('egress_')) hooks.push(e);
});

async function egress(method, body, video = { roomRecord: true }) {
  const r = await fetch(`${API}/twirp/livekit.Egress/${method}`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token('rec', video)}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return r.json();
}
const brief = (i) => i.code ? `error ${i.code}: ${i.msg}` : `${i.status}`;

const p = await (await browser.newContext()).newPage();
p.on('pageerror', (e) => console.log('pageerror', e.message));
await p.goto(PAGE);
await p.evaluate(async ([url, tok]) => {
  const L = LivekitClient;
  const r = new L.Room();
  window.r = r;
  await r.connect(url, tok);
  await r.localParticipant.setMicrophoneEnabled(true);
  await r.localParticipant.setCameraEnabled(true, { resolution: L.VideoPresets.h720.resolution }, { simulcast: true });
}, [LK, token('alice', grant(room))]);
await sleep(2000);

const tracks = (await twirp('ListParticipants', { room }, room)).participants[0].tracks;
const mic = tracks.find((t) => t.type === 'AUDIO').sid;
const cam = tracks.find((t) => t.type === 'VIDEO').sid;

console.log('no roomRecord:', brief(await egress('StartTrackEgress', { room_name: room, track_id: mic }, { roomAdmin: true, room })));
console.log('no such track:', brief(await egress('StartTrackEgress', { room_name: room, track_id: 'TR_nope', file: { filepath: 'x' } })));
console.log('no such room:', brief(await egress('StartTrackEgress', { room_name: 'nope', track_id: mic, file: { filepath: 'x' } })));
console.log('path outside:', brief(await egress('StartTrackEgress', { room_name: room, track_id: mic, file: { filepath: '../x' } })));
console.log('websocket:', brief(await egress('StartTrackEgress', { room_name: room, track_id: mic, websocket_url: 'ws://x' })));
console.log('room composite:', brief(await egress('StartRoomCompositeEgress', { room_name: room })));

const a = await egress('StartTrackEgress', { room_name: room, track_id: mic, file: { filepath: '{room_name}/{track_type}-{publisher_identity}' } });
const v = await egress('StartTrackEgress', { room_name: room, track_id: cam, file: { filepath: '' } });
console.log('audio start:', brief(a), a.file_results[0].filename.replace(room, 'ROOM'));
console.log('video start:', brief(v), /^TR_\w+-\d{4}-\d\d-\d\dT\d{6}\.webm$/.test(v.file_results[0].filename) ? 'TRACK-TIME.webm' : v.file_results[0].filename);
await sleep(1000);
const recording = async () => (await twirp('ListRooms', { names: [room] }, room)).rooms[0].active_recording;
console.log('active_recording:', await recording());
const active = await egress('ListEgress', { room_name: room, active: true });
console.log('active:', active.items.map((i) => `${i.egress_id === a.egress_id ? 'audio' : 'video'} ${i.status}`).sort().join(', '));

// Muted for 2 s mid-way: the audio file fills the gap with silence, so
// both files last as long as the recording.
await sleep(3000);
const mute = (on) => p.evaluate(async (on) => {
  for (const pub of window.r.localParticipant.trackPublications.values()) await (on ? pub.mute() : pub.unmute());
}, on);
await mute(true);
await sleep(2000);
await mute(false);
await sleep(3000);
console.log('video stop:', brief(await egress('StopEgress', { egress_id: v.egress_id })));
await p.evaluate(async () => {
  const pub = [...window.r.localParticipant.audioTrackPublications.values()][0];
  await window.r.localParticipant.unpublishTrack(pub.track);
});
await sleep(2000);
console.log('video stop again:', brief(await egress('StopEgress', { egress_id: v.egress_id })));
console.log('stop unknown:', brief(await egress('StopEgress', { egress_id: 'EG_nope' })));
console.log('active_recording:', await recording());

const all = (await egress('ListEgress', { room_name: room })).items;
for (const [name, id] of [['audio', a.egress_id], ['video', v.egress_id]]) {
  const i = all.find((x) => x.egress_id === id);
  const f = i.file_results[0];
  console.log(`${name}: ${i.status}, ${i.details}, ${(Number(f.duration) / 1e9).toFixed(0)} s`);
  console.log(`${name} webhooks:`, hooks.filter((e) => e.egressInfo.egressId === id).map((e) => `${e.event}:${e.egressInfo.status ?? "EGRESS_STARTING"}`).join(' '));
  const probe = JSON.parse(execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration,format_name:stream=codec_name,width,height', '-of', 'json', f.location]));
  const s = probe.streams[0];
  const errors = execFileSync('ffmpeg', ['-v', 'error', '-i', f.location, '-f', 'null', '-'], { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
  console.log(`${name} file: ${probe.format.format_name} ${s.codec_name}${s.height ? ` ${s.height}p` : ''}, ${Number(probe.format.duration).toFixed(0)} s, ${errors ? `errors: ${errors.split('\n').length}` : 'decodes cleanly'}`);
}
receiver.close();
await browser.close();
