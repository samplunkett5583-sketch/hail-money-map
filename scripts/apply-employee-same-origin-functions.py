from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
old = """    function hmEmployeeFunctionUrl(functionName) {
      var local = location.hostname === '127.0.0.1' || location.hostname === 'localhost';
      return local ? 'http://127.0.0.1:5015/hailmoneymap/us-central1/' + functionName : 'https://us-central1-hailmoneymap.cloudfunctions.net/' + functionName;
    }"""
new = """    function hmEmployeeFunctionUrl(functionName) {
      var local = location.hostname === '127.0.0.1' || location.hostname === 'localhost';
      return local ? 'http://127.0.0.1:5015/hailmoneymap/us-central1/' + functionName : '/api/employee/' + encodeURIComponent(functionName);
    }"""
if "/api/employee/" not in text:
    if old not in text:
        raise SystemExit('Employee function URL helper not found')
    text = text.replace(old, new, 1)
path.write_text(text, encoding='utf-8')
print('Employee functions now use same-origin hosting routes.')
