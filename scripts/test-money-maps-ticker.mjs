import assert from 'node:assert/strict';
import fs from 'node:fs';

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
assert.match(html, /HMM_TICKER_PIXELS_PER_SECOND\s*=\s*18/, 'ticker speed must use the readable 18px/s target');
assert.match(html, /travelPx\s*\/\s*HMM_TICKER_PIXELS_PER_SECOND/, 'ticker duration must be calculated from measured width');
assert.match(html, /mouseenter[\s\S]*hmmTickerSetPause\('hover', true\)/);
assert.match(html, /mouseleave[\s\S]*hmmTickerSetPause\('hover', false\)/);
assert.match(html, /pointerdown[\s\S]*hmmTickerSetPause\('press', true\)/);
assert.match(html, /pointercancel/);
assert.match(html, /focusin[\s\S]*hmmTickerSetPause\('focus', true\)/);
assert.match(html, /prefers-reduced-motion: reduce/);
assert.match(html, /matchMedia\('\(prefers-reduced-motion: reduce\)'\)/);
assert.match(html, /reason !== 'reduced-motion'/, 'reduced motion must stop animation without blocking alert text updates');
assert.match(html, /tabindex="0" aria-label="Hurricane center and severe weather alerts"/);
assert.equal((html.match(/id="maps-hurricane-ticker-text"/g) || []).length, 1, 'ticker DOM must not be duplicated');
console.log('Money Maps ticker regression checks passed: 18px/s, width-based duration, hover/press/focus/reduced-motion pause.');
