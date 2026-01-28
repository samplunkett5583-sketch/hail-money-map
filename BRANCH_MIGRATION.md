# Default Branch Migration: master → main

## Problem

The GitHub Copilot workflow fails with the following error:

```
fatal: ambiguous argument 'refs/heads/master': unknown revision or path not in the working tree.
```

This occurs because:
1. The repository's default branch on GitHub is set to `master`
2. The Copilot workflow expects to diff against the default branch
3. But the workflow checkout doesn't fetch the `master` branch locally

## Solution

Migrate the repository's default branch from `master` to `main`.

### Steps to Fix

#### 1. Create the `main` branch

**Option A: Use GitHub Actions (Recommended)**

1. Go to the Actions tab in your repository
2. Find the "Create Main Branch" workflow
3. Click "Run workflow" to trigger it manually
4. The workflow will automatically create and push the `main` branch

**Option B: Use the provided script**

```bash
./scripts/create-main-branch.sh
```

**Option C: Manual creation**

```bash
# Fetch master if needed
git fetch origin master:master

# Create and push main branch
git checkout -b main master
git push -u origin main
```

#### 2. Update GitHub Repository Settings

1. Navigate to: https://github.com/samplunkett5583-sketch/hail-money-map/settings/branches
2. Click "Switch default branch" or the edit icon next to the default branch
3. Select `main` as the new default branch
4. Confirm the change

#### 3. Verify

After changing the default branch:
- GitHub Copilot workflows will use `main` as the base branch
- Git diff operations will work correctly
- All automations will reference the new default branch

## Why This Fix Works

- **No Code Changes Needed**: The repository code has no hardcoded `master` references
- **GitHub Configuration**: The issue is at the repository settings level, not in the code
- **Copilot Integration**: GitHub Copilot workflows automatically use `github.event.repository.default_branch`
- **Industry Standard**: `main` is now the standard default branch name for Git repositories

## Impact

- ✅ Fixes GitHub Copilot workflow failures
- ✅ Aligns with modern Git conventions
- ✅ No breaking changes to existing code
- ✅ Workflows automatically adapt to the new default branch
