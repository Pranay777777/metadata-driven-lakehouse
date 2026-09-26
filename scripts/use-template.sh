#!/usr/bin/env bash
# Rename the scaffold for a new project.
#   ./scripts/use-template.sh my-project my_package "One-line description"
set -euo pipefail

REPO="${1:?usage: use-template.sh <repo-name> <package_name> <description>}"
PKG="${2:?missing package name}"
DESC="${3:?missing description}"

if [ "$PKG" != "app" ]; then
  git mv src/app "src/$PKG"
  grep -rl --exclude-dir=.git -e 'app\.' -e '"app"' -e "'app'" -e 'module app' . 2>/dev/null \
    | while read -r f; do
        sed -i "s/\bapp\./$PKG./g; s/\"app\"/\"$PKG\"/g; s/'app'/'$PKG'/g" "$f"
      done
  sed -i "s/^name = \"app\"/name = \"$PKG\"/" pyproject.toml
  sed -i "s/python -m app/python -m $PKG/g" Makefile Dockerfile "src/$PKG/__main__.py"
  sed -i "s/import app/import $PKG/g" .github/workflows/ci.yml Dockerfile
fi

sed -i "s/PROJECT_DESCRIPTION/$DESC/" pyproject.toml
sed -i "s/PROJECT_NAME/$REPO/g; s|Pranay777777/REPO|Pranay777777/$REPO|g" README.md

echo "Renamed to $REPO (package: $PKG)."
echo "Next: fill in the README, then run 'make install && make test'."
