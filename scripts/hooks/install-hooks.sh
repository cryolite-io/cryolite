#!/bin/bash
# Install Git hooks for CRYOLITE
# This script copies hooks from scripts/hooks/ to .git/hooks/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOOKS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)/.git/hooks"

echo "🔧 Installing Git hooks..."

# Check if we're in a git repository
if [ ! -d "$(cd "$SCRIPT_DIR/../.." && pwd)/.git" ]; then
  echo "❌ Error: Not in a Git repository"
  exit 1
fi

# Copy pre-commit hook
echo "📝 Installing pre-commit hook..."
cp "$SCRIPT_DIR/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"

# Copy commit-msg hook
echo "📝 Installing commit-msg hook..."
cp "$SCRIPT_DIR/commit-msg" "$HOOKS_DIR/commit-msg"
chmod +x "$HOOKS_DIR/commit-msg"

echo "✅ Git hooks installed successfully!"
echo ""
echo "Installed hooks:"
echo "  - pre-commit: Code formatting, tests, SonarCloud analysis"
echo "  - commit-msg: Conventional Commits validation"
echo ""
echo "To verify installation:"
echo "  ls -la .git/hooks/pre-commit .git/hooks/commit-msg"
echo ""
echo "To test hooks manually:"
echo "  .git/hooks/pre-commit"
echo "  echo 'feat: test' | .git/hooks/commit-msg /dev/stdin"

