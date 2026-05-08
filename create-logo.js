const sharp = require('sharp');
const fs = require('fs');
const path = require('path');

const svgContent = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" width="48" height="48">
  <circle cx="24" cy="24" r="22" fill="#16a34a"/>
  <text x="24" y="27" font-size="28" font-weight="bold" fill="#facc15" text-anchor="middle" dominant-baseline="middle" font-family="Arial, sans-serif">$</text>
</svg>`;

const assetDir = path.join(__dirname, 'public', 'assets');
if (!fs.existsSync(assetDir)) {
  fs.mkdirSync(assetDir, { recursive: true });
}

sharp(Buffer.from(svgContent))
  .png()
  .toFile(path.join(assetDir, 'hmm-globe.png'))
  .then(() => console.log('✓ Created: public/assets/hmm-globe.png'))
  .catch(err => console.error('✗ Error:', err.message));
