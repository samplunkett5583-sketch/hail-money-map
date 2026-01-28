#!/bin/bash
# Script to create main branch from master for GitHub default branch migration
# This addresses the issue where GitHub Copilot workflows fail due to master branch references

set -e

echo "Creating 'main' branch from 'master'..."

# Fetch master branch if not already present
if ! git rev-parse --verify master >/dev/null 2>&1; then
    echo "Fetching master branch from origin..."
    git fetch origin master:master
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# Create main branch from master
if git rev-parse --verify main >/dev/null 2>&1; then
    echo "main branch already exists locally"
else
    echo "Creating main branch from master..."
    git branch main master
fi

# Push main branch
echo "Pushing main branch to origin..."
git push -u origin main
echo "✓ main branch created and pushed"

echo ""
echo "Next steps:"
echo "1. Go to your repository settings page: Settings > Branches"
echo "2. Change the default branch from 'master' to 'main'"
echo "3. This will resolve the GitHub Copilot workflow git diff errors"
