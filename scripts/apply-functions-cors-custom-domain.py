from pathlib import Path

path = Path('functions/index.js')
text = path.read_text(encoding='utf-8')

old = '''  const allowed = /^https:\\/\\/(hailmoneymap\\.web\\.app|hailmoneymap\\.firebaseapp\\.com)$/i.test(origin) ||
    /^http:\\/\\/(127\\.0\\.0\\.1|localhost):\\d+$/i.test(origin);'''
new = '''  const allowed = /^https:\\/\\/(hailmoneymap\\.web\\.app|hailmoneymap\\.firebaseapp\\.com|(?:www\\.)?hail\\.money)$/i.test(origin) ||
    /^http:\\/\\/(127\\.0\\.0\\.1|localhost):\\d+$/i.test(origin);'''

if '(?:www\\.)?hail\\.money' not in text:
    if old not in text:
        raise SystemExit('permitCors allow-list block not found')
    text = text.replace(old, new, 1)

path.write_text(text, encoding='utf-8')
print('Firebase function CORS now allows hail.money and www.hail.money.')
