// Raw Twirp JSON, normalised: keys sorted, volatile values masked.
import { LK, PAGE, launch, token as tok } from './lib.mjs';
const admin = tok('admin', { roomList: true, roomCreate: true, roomAdmin: true, room: 'x' });
const call = async (m, b) => JSON.parse(await (await fetch(`http://localhost:7880/twirp/livekit.RoomService/${m}`, { method: 'POST', headers: { Authorization: `Bearer ${admin}`, 'Content-Type': 'application/json' }, body: JSON.stringify(b) })).text());
const VOLATILE = /^(sid|creation_time|creation_time_ms|joined_at|joined_at_ms|turn_password|node_id|version|cid|mid|stream|ssrc|repair_ssrc|width|height|mime_type|unix_micro|rid|bitrate|width|height)$/;
const norm = (v, k = '') => Array.isArray(v) ? v.map((x) => norm(x)) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map((key) => [key, norm(v[key], key)])) : VOLATILE.test(k) && v !== null && v !== '' ? '<v>' : v;
const browser = await launch();
await call('CreateRoom', { name: 'x', metadata: 'm' });
const page = await (await browser.newContext()).newPage();
await page.goto(PAGE);
await page.evaluate(async ([url, t]) => { const r = new LivekitClient.Room(); await r.connect(url, t); await r.localParticipant.setMicrophoneEnabled(true); await r.localParticipant.setCameraEnabled(true); }, [LK, tok('alice', { room: 'x', roomJoin: true })]);
await new Promise((r) => setTimeout(r, 6000));
for (const [m, b] of [['ListRooms', { names: ['x'] }], ['ListParticipants', { room: 'x' }], ['MutePublishedTrack', null], ['UpdateRoomMetadata', { room: 'x', metadata: 'n' }]]) {
  if (m === 'MutePublishedTrack') { const p = (await call('ListParticipants', { room: 'x' })).participants[0]; b && 0; console.log(m, JSON.stringify(norm(await call(m, { room: 'x', identity: 'alice', track_sid: p.tracks[0].sid, muted: true })), null, 1)); continue; }
  console.log(m, JSON.stringify(norm(await call(m, b)), null, 1));
}
await browser.close();
