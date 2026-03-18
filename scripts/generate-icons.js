/**
 * generate-icons.js
 *
 * Generates SVG-based PNG icon files for public/assets/.
 * Requires `sharp` to be installed: npm install sharp
 *
 * Usage: node scripts/generate-icons.js
 *
 * NOTE: The inline SVGs defined here have been applied directly to
 * public/index.html as inline SVG elements (replacing broken stub PNGs).
 * This script exists for future use if PNG exports are needed.
 */

import sharp from 'sharp';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, '..', 'public', 'assets');

const icons = [
  {
    name: 'new-lead-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 36 36" xmlns="http://www.w3.org/2000/svg">
      <rect width="36" height="36" rx="8" fill="#0b1220"/>
      <circle cx="14" cy="13" r="5" fill="none" stroke="#ff8c00" stroke-width="2.2"/>
      <path d="M4 32c0-5.5 4.5-9 10-9s10 3.5 10 9" fill="none" stroke="#ff8c00" stroke-width="2.2" stroke-linecap="round"/>
      <line x1="27" y1="8" x2="27" y2="18" stroke="#ff8c00" stroke-width="2.5" stroke-linecap="round"/>
      <line x1="22" y1="13" x2="32" y2="13" stroke="#ff8c00" stroke-width="2.5" stroke-linecap="round"/>
    </svg>`,
  },
  {
    name: 'estimates-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 36 36" xmlns="http://www.w3.org/2000/svg">
      <rect width="36" height="36" rx="8" fill="#0b1220"/>
      <rect x="9" y="6" width="18" height="24" rx="2" fill="#0d1f0d" stroke="#4caf50" stroke-width="1.8"/>
      <rect x="14" y="4" width="8" height="5" rx="1.5" fill="#0b1220" stroke="#4caf50" stroke-width="1.5"/>
      <text x="18" y="26" font-size="16" font-weight="bold" fill="#4caf50" text-anchor="middle" font-family="Arial,sans-serif">$</text>
      <line x1="12" y1="17" x2="24" y2="17" stroke="#4caf50" stroke-width="1.2" opacity="0.6"/>
      <line x1="12" y1="21" x2="21" y2="21" stroke="#4caf50" stroke-width="1.2" opacity="0.6"/>
    </svg>`,
  },
  {
    name: 'downloads-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 36 36" xmlns="http://www.w3.org/2000/svg">
      <rect width="36" height="36" rx="8" fill="#0b1220"/>
      <line x1="18" y1="6" x2="18" y2="22" stroke="#4fc3f7" stroke-width="2.5" stroke-linecap="round"/>
      <polyline points="11,16 18,23 25,16" fill="none" stroke="#4fc3f7" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
      <rect x="7" y="26" width="22" height="4" rx="2" fill="#4fc3f7" opacity="0.85"/>
    </svg>`,
  },
  {
    name: 'settings-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <rect width="24" height="24" rx="4" fill="#0b1220"/>
      <path d="M19.14,12.94c.04-.3.06-.61.06-.94s-.02-.64-.07-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94L14.4,2.81c-.04-.24-.24-.41-.48-.41h-3.84c-.24,0-.43.17-.47.41L9.25,5.35c-.59.24-1.13.56-1.62.94L5.24,5.33c-.22-.08-.47,0-.59.22L2.74,8.87c-.12.21-.08.47.12.61l2.03,1.58C4.84,11.36,4.8,11.69,4.8,12s.02.64.07.94l-2.03,1.58c-.18.14-.23.41-.12.61l1.92,3.32c.12.22.37.29.59.22l2.39-.96c.5.38,1.03.7,1.62.94l.36,2.54c.05.24.24.41.48.41h3.84c.24,0,.44-.17.47-.41l.36-2.54c.59-.24,1.13-.56,1.62-.94l2.39.96c.22.08.47,0,.59-.22l1.92-3.32c.12-.22.07-.47-.12-.61L19.14,12.94zM12,15.6c-1.98,0-3.6-1.62-3.6-3.6s1.62-3.6,3.6-3.6,3.6,1.62,3.6,3.6S13.98,15.6,12,15.6z" fill="#9e9e9e"/>
    </svg>`,
  },
  {
    name: 'get-directions-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 44 44" xmlns="http://www.w3.org/2000/svg">
      <rect width="44" height="44" rx="10" fill="#0b1220"/>
      <polygon points="22,6 38,38 22,31 6,38" fill="#00bcd4" opacity="0.85"/>
      <polygon points="22,6 38,38 22,31" fill="#00acc1"/>
      <circle cx="22" cy="22" r="3" fill="#fff" opacity="0.9"/>
    </svg>`,
  },
  {
    name: 'start-inspection-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 44 44" xmlns="http://www.w3.org/2000/svg">
      <rect width="44" height="44" rx="10" fill="#0b1220"/>
      <rect x="12" y="11" width="20" height="26" rx="3" fill="none" stroke="#26a69a" stroke-width="2"/>
      <rect x="17" y="8" width="10" height="7" rx="2" fill="#0b1220" stroke="#26a69a" stroke-width="2"/>
      <polyline points="16,26 20,31 29,20" fill="none" stroke="#26a69a" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>`,
  },
  {
    name: 'storm-calendar-icon.png',
    width: 128,
    height: 128,
    svg: `<svg width="128" height="128" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
      <rect x="5" y="15" width="90" height="80" rx="12" fill="#f5a623"/>
      <rect x="5" y="10" width="90" height="18" rx="8" fill="#2196F3"/>
      <circle cx="30" cy="12" r="7" fill="#0d47a1" stroke="#64b5f6" stroke-width="2"/>
      <circle cx="70" cy="12" r="7" fill="#0d47a1" stroke="#64b5f6" stroke-width="2"/>
      <rect x="12" y="38" width="76" height="4" rx="2" fill="rgba(0,0,0,0.15)"/>
      <circle cx="50" cy="65" r="26" fill="#1a8c4e" stroke="#f5a623" stroke-width="3"/>
      <ellipse cx="50" cy="65" rx="12" ry="26" fill="none" stroke="rgba(255,255,255,0.4)" stroke-width="1.5"/>
      <line x1="24" y1="65" x2="76" y2="65" stroke="rgba(255,255,255,0.4)" stroke-width="1.5"/>
      <text x="50" y="72" font-size="28" font-weight="bold" fill="#f5a623" text-anchor="middle" font-family="Arial,sans-serif" stroke="#7b3a00" stroke-width="1">$</text>
    </svg>`,
  },
];

async function generateIcons() {
  for (const icon of icons) {
    const outPath = path.join(OUT_DIR, icon.name);
    try {
      await sharp(Buffer.from(icon.svg))
        .resize(icon.width, icon.height)
        .png()
        .toFile(outPath);
      console.log(`✓ Generated ${icon.name}`);
    } catch (err) {
      console.error(`✗ Failed to generate ${icon.name}:`, err.message);
    }
  }
}

generateIcons();
