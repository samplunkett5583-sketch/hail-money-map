from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')

old = "  async function orgId(){return typeof crmResolveFirestoreOrgId==='function'?await crmResolveFirestoreOrgId():''}"
new = """  async function orgId(){
    var id='';
    if(typeof crmResolveFirestoreOrgId==='function'){
      try{id=String(await crmResolveFirestoreOrgId()||'').trim().toLowerCase()}catch(_){}
      if(id)return id;
    }
    var u=window.auth&&window.auth.currentUser;
    if(u){
      try{
        var t=await u.getIdTokenResult(true),c=t&&t.claims||{};
        id=String(c.hmOrganizationId||'').trim().toLowerCase();
        if(id)return id;
      }catch(_){}
      if(window.db){
        try{
          var s=await window.db.collection('hmEmployees').doc(u.uid).get();
          if(s.exists){
            var d=s.data()||{};
            id=String(d.organizationId||d.hmOrganizationId||'').trim().toLowerCase();
            if(id)return id;
          }
        }catch(_){}
      }
      if(/@hailmoney\\.test$/i.test(String(u.email||'')))return'yopro';
    }
    return'';
  }"""

if new in text:
    print('Company Documents workspace resolver already fixed.')
elif old in text:
    text = text.replace(old, new, 1)
    path.write_text(text, encoding='utf-8')
    print('Company Documents workspace resolver fixed.')
else:
    raise SystemExit('Company Documents org resolver anchor not found')
