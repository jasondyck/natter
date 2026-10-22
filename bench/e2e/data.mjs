// Data features with the JS SDK, run against natter or LiveKit on :7880:
// data packets with destinations, text and byte streams, RPC between
// participants. Prints what each participant received, in a stable order.
import { DUAL, LK, PAGE, grant, launch, sleep, token as tok } from './lib.mjs';
const room = `data${Date.now() % 100000}`;
const token = (identity) => tok(identity, grant(room));
const browser = await launch();
const ctx = await browser.newContext();
const pages = {};
for (const name of ['alice', 'bob', 'carol']) {
  const p = await ctx.newPage();
  p.on('pageerror', (e) => console.log(name, 'pageerror', e.message));
  await p.goto(PAGE);
  await p.evaluate(async ([url, tok, dual]) => {
    const L = LivekitClient;
    const r = new L.Room({ singlePeerConnection: !dual });
    window.r = r;
    window.got = [];
    const log = (s) => window.got.push(s);
    r.on(L.RoomEvent.DataReceived, (payload, who, kind, topic) => log(`data ${new TextDecoder().decode(payload)} from=${who?.identity} topic=${topic} kind=${kind}`));
    r.registerTextStreamHandler('chat', async (reader, info) => {
      const text = await reader.readAll();
      log(`text "${text}" from=${info.identity} topic=${reader.info.topic} attrs=${JSON.stringify(reader.info.attributes ?? {})}`);
    });
    r.registerByteStreamHandler('files', async (reader, info) => {
      const chunks = await reader.readAll();
      const n = chunks.reduce((a, c) => a + c.byteLength, 0);
      let sum = 0;
      for (const c of chunks) for (const b of c) sum = (sum + b) % 65521;
      log(`bytes name=${reader.info.name} mime=${reader.info.mimeType} size=${reader.info.size} got=${n} sum=${sum} from=${info.identity}`);
    });
    r.registerRpcMethod('echo', async (d) => `echo:${d.payload}:from=${d.callerIdentity}`);
    r.registerRpcMethod('fail', async () => { throw new L.RpcError(2001, 'custom failure', 'extra'); });
    r.registerRpcMethod('slow', async () => { await new Promise((res) => setTimeout(res, 3000)); return 'late'; });
    await r.connect(url, tok);
  }, [LK, token(name), DUAL]);
  pages[name] = p;
}
await sleep(2000);
const alice = pages.alice;
// Data packets.
await alice.evaluate(async () => {
  const enc = (s) => new TextEncoder().encode(s);
  await window.r.localParticipant.publishData(enc('to-bob'), { reliable: true, destinationIdentities: ['bob'], topic: 't1' });
  await window.r.localParticipant.publishData(enc('to-all-lossy'), { reliable: false, topic: 't2' });
  await window.r.localParticipant.publishData(enc('to-all'), { reliable: true });
});
// Text streams.
await alice.evaluate(async () => {
  const lp = window.r.localParticipant;
  await lp.sendText('hi bob', { topic: 'chat', destinationIdentities: ['bob'] });
  await lp.sendText('hi all', { topic: 'chat', attributes: { k: 'v' } });
  const w = await lp.streamText({ topic: 'chat' });
  for (const part of ['one ', 'two ', 'three']) await w.write(part);
  await w.close();
  // Long text: several chunks.
  await lp.sendText('x'.repeat(40000), { topic: 'chat', destinationIdentities: ['carol'] });
});
// Byte streams.
await alice.evaluate(async () => {
  const lp = window.r.localParticipant;
  const data = new Uint8Array(300 * 1024).map((_, i) => (i * 7) % 251);
  await lp.sendFile(new File([data], 'big.bin', { type: 'application/octet-stream' }), { topic: 'files', destinationIdentities: ['carol'] });
  const w = await lp.streamBytes({ topic: 'files', name: 'streamed', mimeType: 'text/plain', totalSize: 50000 });
  for (let i = 0; i < 5; i++) await w.write(new Uint8Array(10000).fill(i + 1));
  await w.close();
});
await sleep(2000);
// RPC.
const rpc = await alice.evaluate(async () => {
  const lp = window.r.localParticipant;
  const out = [];
  const call = async (label, args) => {
    try { out.push(`${label}: ok ${(await lp.performRpc(args)).slice(0, 60)}`); }
    catch (e) { out.push(`${label}: error ${e.code} ${e.message}${e.data ? ` data=${e.data}` : ''}`); }
  };
  await call('echo', { destinationIdentity: 'bob', method: 'echo', payload: 'p1' });
  await call('big-response-payload', { destinationIdentity: 'bob', method: 'echo', payload: 'y'.repeat(14000) });
  await call('unsupported', { destinationIdentity: 'bob', method: 'nope', payload: '' });
  await call('custom-error', { destinationIdentity: 'bob', method: 'fail', payload: '' });
  await call('timeout', { destinationIdentity: 'bob', method: 'slow', payload: '', responseTimeout: 1500 });
  await call('no-recipient', { destinationIdentity: 'zed', method: 'echo', payload: '' });
  await call('too-large', { destinationIdentity: 'bob', method: 'echo', payload: 'z'.repeat(20000) });
  return out;
});
console.log('== alice rpc');
for (const l of rpc) console.log(`  ${l}`);
await sleep(1000);
for (const [n, p] of Object.entries(pages)) {
  const got = await p.evaluate(() => window.got.splice(0));
  console.log(`== ${n} received`);
  for (const g of got.map((x) => x.length > 160 ? `${x.slice(0, 120)}…(${x.length})` : x).sort()) console.log(`  ${g}`);
}
await browser.close();
