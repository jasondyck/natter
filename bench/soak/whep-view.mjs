// A WHEP viewer for soak.sh: watches $WHEP with token $TOK for 30 s and
// prints the video frames it decoded (0 when it never connected).
import { createRequire } from 'module';
const require = createRequire(process.env.PLAYWRIGHT_DIR ?? `${process.cwd()}/`);
const { chromium } = require('playwright-core');
const browser = await chromium.launch({ executablePath: process.env.CHROME, headless: true });
try {
  const p = await browser.newPage();
  await p.goto(process.env.PAGE ?? 'http://localhost:8766/index.html');
  const frames = await p.evaluate(async ([url, tok]) => {
    const pc = new RTCPeerConnection();
    pc.addTransceiver('video', { direction: 'recvonly' });
    await pc.setLocalDescription(await pc.createOffer());
    await new Promise((ok) => {
      if (pc.iceGatheringState === 'complete') return ok();
      pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && ok();
    });
    const r = await fetch(url, {
      method: 'POST', body: pc.localDescription.sdp,
      headers: { 'Content-Type': 'application/sdp', Authorization: `Bearer ${tok}` },
    });
    if (r.status !== 201) return 0;
    const location = r.headers.get('Location');
    await pc.setRemoteDescription({ type: 'answer', sdp: await r.text() });
    await new Promise((ok) => setTimeout(ok, 30000));
    let frames = 0;
    for (const s of (await pc.getStats()).values()) if (s.type === 'inbound-rtp') frames = s.framesDecoded ?? 0;
    await fetch(new URL(location, url), { method: 'DELETE', headers: { Authorization: `Bearer ${tok}` } });
    return frames;
  }, [process.env.WHEP, process.env.TOK]);
  console.log(frames);
} finally {
  await browser.close();
}
