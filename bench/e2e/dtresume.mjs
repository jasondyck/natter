// Data tracks across a signal resume: dtrack.mjs with DT_RESUME=1.
process.env.DT_RESUME = '1';
await import('./dtrack.mjs');
