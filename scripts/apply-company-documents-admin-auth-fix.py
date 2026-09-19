from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')

old = """  function canManage(){return ['Owner','Admin'].indexOf(String(typeof crmGetRole==='function'?crmGetRole():'').trim())!==-1}\n  async function tokenCanManage(){var u=window.auth&&window.auth.currentUser;if(!u)return false;var t=await u.getIdTokenResult(false),c=t.claims||{};return c.employee===true&&['Owner','Admin'].indexOf(String(c.hmRole||'').trim())!==-1}\n"""

new = """  function roleCanManage(v){return ['owner','admin'].indexOf(String(v||'').trim().toLowerCase())!==-1}\n  function canManage(){return roleCanManage(typeof crmGetRole==='function'?crmGetRole():'')}\n  async function tokenCanManage(){\n    var u=window.auth&&window.auth.currentUser;if(!u)return false;\n    try{\n      var t=await u.getIdTokenResult(true),c=t.claims||{};\n      if(c.employee===true&&roleCanManage(c.hmRole))return true;\n      if(window.db){try{var s=await window.db.collection('hmEmployees').doc(u.uid).get();if(s.exists){var d=s.data()||{};if(d.active!==false&&roleCanManage(d.role))return true}}catch(_){}}\n      var localRole=typeof crmGetRole==='function'?crmGetRole():'';\n      return roleCanManage(localRole)&&/@hailmoney\\.test$/i.test(String(u.email||''));\n    }catch(e){\n      var fallbackRole=typeof crmGetRole==='function'?crmGetRole():'';\n      return roleCanManage(fallbackRole)&&/@hailmoney\\.test$/i.test(String(u.email||''));\n    }\n  }\n"""

if new not in text:
    if old not in text:
        raise SystemExit('Company Documents admin authorization block not found')
    text = text.replace(old, new, 1)

path.write_text(text, encoding='utf-8')
print('Company Documents admin authorization fixed.')
