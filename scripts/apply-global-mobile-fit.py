from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = '/* HAIL MONEY GLOBAL MOBILE FIT V1 */'
if marker in text:
    raise SystemExit('Global mobile fit already applied')

css = r'''
<style>
/* HAIL MONEY GLOBAL MOBILE FIT V1 */
@media (max-width: 760px) {
  html, body { width: 100%; max-width: 100%; overflow-x: hidden !important; }
  *, *::before, *::after { box-sizing: border-box; }

  main, .page, [id^="page-"], .placeholder-wrap,
  .main-menu-card, .estimates-gold-card, .est-ai-result-card,
  .estimates-panel, .panel, .card, .content, .content-wrap {
    max-width: 100% !important;
    min-width: 0 !important;
  }

  input, select, textarea, button {
    max-width: 100%;
  }

  img, canvas, video, iframe, svg {
    max-width: 100%;
  }

  dialog {
    width: calc(100vw - 20px) !important;
    max-width: calc(100vw - 20px) !important;
    max-height: calc(100dvh - 20px) !important;
    margin: auto !important;
    overflow: auto !important;
  }

  #hm-estimate-crm-dialog form {
    padding: 16px !important;
    gap: 12px !important;
  }
  #hm-estimate-crm-dialog form > div,
  #hm-estimate-crm-dialog form div[style*="grid-template-columns"] {
    grid-template-columns: 1fr !important;
    width: 100% !important;
  }
  #hm-estimate-crm-dialog label,
  #hm-estimate-crm-dialog input,
  #hm-estimate-crm-dialog select {
    width: 100% !important;
    min-width: 0 !important;
  }
  #hm-estimate-crm-dialog form > div:last-child {
    display: grid !important;
    grid-template-columns: 1fr 1fr !important;
    gap: 10px !important;
  }
  #hm-estimate-crm-dialog form > div:last-child button {
    width: 100% !important;
    min-width: 0 !important;
  }

  .est-ai-detail-grid,
  .est-ai-measurement-summary,
  .est-ai-aerial-metrics,
  .est-ai-review-grid,
  .est-ai-textarea-grid,
  .est-ai-totals,
  .est-ai-totals-controls,
  .est-ai-source-grid {
    grid-template-columns: 1fr !important;
    width: 100% !important;
    min-width: 0 !important;
  }

  .est-ai-table-wrap,
  .table-wrap,
  .table-responsive,
  [class*="table-wrap"] {
    width: 100% !important;
    max-width: 100% !important;
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
  }

  .est-ai-result-toolbar,
  .page-actions,
  .est-ai-review-actions,
  .est-ai-save-row {
    max-width: 100% !important;
    flex-wrap: wrap !important;
  }

  /* Keep account/profile menus inside the right edge of the viewport. */
  [id*="profile" i][class*="menu" i],
  [id*="profile" i][class*="dropdown" i],
  [class*="profile" i][class*="menu" i],
  [class*="profile" i][class*="dropdown" i],
  [id*="account" i][class*="menu" i],
  [class*="account" i][class*="dropdown" i],
  [class*="user" i][class*="dropdown" i] {
    max-width: calc(100vw - 16px) !important;
    min-width: min(240px, calc(100vw - 16px)) !important;
    right: 8px !important;
    left: auto !important;
  }
}
</style>
'''

js = r'''
<script>
(function(){
  'use strict';
  function isMobile(){ return window.innerWidth <= 760; }
  function visible(el){
    if (!el || !(el instanceof HTMLElement)) return false;
    var s = getComputedStyle(el);
    return s.display !== 'none' && s.visibility !== 'hidden' && el.getClientRects().length > 0;
  }
  function likelyFloating(el){
    var id = String(el.id || '').toLowerCase();
    var cls = String(el.className || '').toLowerCase();
    var role = String(el.getAttribute('role') || '').toLowerCase();
    var name = id + ' ' + cls + ' ' + role;
    return /menu|dropdown|popover|profile|account|user-menu|context-menu/.test(name);
  }
  function clampFloatingMenus(){
    if (!isMobile()) return;
    document.querySelectorAll('body *').forEach(function(el){
      if (!visible(el) || !likelyFloating(el)) return;
      var pos = getComputedStyle(el).position;
      if (pos !== 'absolute' && pos !== 'fixed') return;
      var r = el.getBoundingClientRect();
      if (r.width > window.innerWidth - 16) {
        el.style.setProperty('width', 'calc(100vw - 16px)', 'important');
        el.style.setProperty('max-width', 'calc(100vw - 16px)', 'important');
      }
      if (r.right > window.innerWidth - 8 || r.left < 8) {
        el.style.setProperty('right', '8px', 'important');
        el.style.setProperty('left', 'auto', 'important');
        var rr = el.getBoundingClientRect();
        if (rr.left < 8) {
          el.style.setProperty('left', '8px', 'important');
          el.style.setProperty('right', '8px', 'important');
          el.style.setProperty('width', 'auto', 'important');
        }
      }
    });
  }
  var scheduled = false;
  function schedule(){
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(function(){ scheduled = false; clampFloatingMenus(); });
  }
  document.addEventListener('click', function(){ setTimeout(schedule, 0); setTimeout(schedule, 80); }, true);
  window.addEventListener('resize', schedule, {passive:true});
  window.addEventListener('orientationchange', schedule, {passive:true});
  new MutationObserver(schedule).observe(document.documentElement, {subtree:true, childList:true, attributes:true, attributeFilter:['class','style','hidden','open']});
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', schedule); else schedule();
})();
</script>
'''

if '</head>' not in text or '</body>' not in text:
    raise SystemExit('Expected head/body closing tags not found')
text = text.replace('</head>', css + '\n</head>', 1)
text = text.replace('</body>', js + '\n</body>', 1)
path.write_text(text, encoding='utf-8')
print('Applied global mobile viewport fit and floating-menu clamping')
