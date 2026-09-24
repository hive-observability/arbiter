#!/usr/bin/env bash
# Build exactly the release tarballs in an independent Cabal project, without
# the workspace's tests, benchmarks, demo, or bench packages. On GHC 9.14,
# only relax known upstream bounds on its bundled base/template-haskell;
# Arbiter's own package bounds remain enforced.
set -euo pipefail

: "${RELEASE_PACKAGES:?Set RELEASE_PACKAGES to the packages being published}"

repo=$(pwd)
project=$(mktemp -d)
trap 'rm -rf "$project"' EXIT

printf 'packages:\n' > "$project/cabal.project"
for pkg in $RELEASE_PACKAGES; do
  version=$(awk '/^version:/ { print $2; exit }' "$repo/$pkg/$pkg.cabal")
  tarball="$repo/dist-newstyle/sdist/${pkg}-${version}.tar.gz"
  test -f "$tarball" || { echo "Missing release tarball: $tarball" >&2; exit 1; }
  tar -xzf "$tarball" -C "$project"
  printf '  %s\n' "${pkg}-${version}" >> "$project/cabal.project"
done

(
  cd "$project"
  if [[ $(ghc --numeric-version) == 9.14.* ]]; then
    cabal build all --disable-tests --disable-benchmarks \
      --allow-newer=postgresql-simple:base,postgresql-simple:template-haskell,insert-ordered-containers:base \
      "$@"
  else
    cabal build all --disable-tests --disable-benchmarks "$@"
  fi
)
