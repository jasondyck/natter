// What the scenario scripts share: playwright, the server and page URLs,
// dev-key tokens, the browser, RoomService calls and a webhook receiver.
import { createRequire } from 'module';
import crypto from 'crypto';
import http from 'http';

const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
export const { chromium } = require('playwright-core');
export const LK = process.env.LK_URL ?? 'ws://localhost:7880';
export const PAGE = process.env.PAGE ?? 'http://localhost:8766/index.html';
export const DUAL = !!process.env.DUAL;
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const b64 = (b) => Buffer.from(b).toString('base64url');

/** An HS256 token for the dev keys (devkey/secret): `video` is the grant,
 * `claims` any other claims (attributes, metadata, kind…). */
export function token(identity, video, claims = {}, ttl = 3600) {
  const now = Math.floor(Date.now() / 1000);
  const h = b64(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const p = b64(JSON.stringify({ iss: 'devkey', sub: identity, name: identity, nbf: now, exp: now + ttl, video, ...claims }));
  return `${h}.${p}.${crypto.createHmac('sha256', 'secret').update(`${h}.${p}`).digest('base64url')}`;
}

/** The usual participant grant for `room`: join, publish, subscribe, data. */
export const grant = (room, extra = {}) => ({ room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true, ...extra });

/** Headless Chrome with fake media devices. */
export function launch(args = []) {
  return chromium.launch({
    executablePath: process.env.CHROME, headless: true,
    args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--autoplay-policy=no-user-gesture-required', ...args],
  });
}

/** A RoomService call with an admin token for `room`. */
export async function twirp(method, body, room) {
  const t = token('admin', { room, roomAdmin: true, roomList: true, roomCreate: true });
  const r = await fetch(`http://localhost:7880/twirp/livekit.RoomService/${method}`, {
    method: 'POST', headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  return r.json();
}

/** Receives webhooks on :8797 (the configs' URL), calling `onEvent` with
 * each parsed event. */
export function webhooks(onEvent) {
  return http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      onEvent(JSON.parse(body));
      res.end();
    });
  }).listen(8797, '127.0.0.1');
}
