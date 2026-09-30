# Hail Money

## Canonical Production Source

- The only working Hail Money repository is `C:\dev\HailMoney`.
- The canonical branch is `master`.
- The live application is `https://www.hail.money`.
- The active web source is `C:\dev\HailMoney\public`.
- The entry file is `C:\dev\HailMoney\public\index.html`.
- The locked production tag is `PRODUCTION_LOCKED_2026-10-01`.

Do not deploy from old worktrees, backups, staging folders, root HTML copies, or previous builds. For targeted changes, modify only the canonical production source and verify the live site after deployment.

## Hosting Deployment

From `C:\dev\HailMoney`, deploy Hosting only unless a backend change was explicitly requested:

```powershell
.\node_modules\.bin\firebase.cmd deploy --only hosting --project hailmoneymap
```



