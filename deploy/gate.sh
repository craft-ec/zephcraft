#!/usr/bin/env bash
# ── ZephCraft pre-deploy gate ────────────────────────────────────────────────
# Run from anywhere; MUST pass before ANY fleet roll (see deploy/README.md and
# the zeph-fleet-deploy memory).
#
# WHY this exists: the A-G acceptance harness (tests/tests/transfer_plane.rs,
# the `--ignored` scenarios) is NOT the whole test surface. The transport unit
# tests and the noded subprocess tests (crates/transport, crates/noded/tests)
# live OUTSIDE that harness, and a wire migration once left them RED unnoticed
# (the ping->mux migration: unit tests bound the retired ALPN, a subprocess test
# asserted a dropped log). The A-G suite stayed green the whole time. This gate
# runs the COMPLETE local surface so that can't happen again.
#
# Usage:
#   deploy/gate.sh            full gate (fmt + clippy + workspace tests + A-G)
#   deploy/gate.sh --quick    skip the ~11min A-G harness — LOCAL-LOGIC changes
#                             ONLY; NEVER for wire/ALPN/transport/seed changes.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2 # -> zephcraft/ workspace root

quick=0
[[ "${1:-}" == "--quick" ]] && quick=1

fail=0
step() { printf '\n━━━ %s ━━━\n' "$1"; }
ok()   { printf '✅ %s\n' "$1"; }
bad()  { printf '❌ %s\n' "$1"; fail=1; }

step "1/4  fmt"
if cargo fmt --all -- --check; then ok "fmt clean"; else bad "fmt — run 'cargo fmt --all'"; fi

step "2/4  clippy (-D warnings, all targets incl. tests)"
if cargo clippy --workspace --all-targets -- -D warnings; then ok "clippy clean"; else bad "clippy"; fi

step "3/4  workspace tests (transport unit + noded subprocess + integration — NOT --ignored)"
if cargo test --workspace; then ok "workspace tests"; else bad "workspace tests"; fi

if [[ $quick -eq 1 ]]; then
  printf '\n(--quick: SKIPPED the A-G acceptance harness. Do NOT skip for wire/ALPN/transport/seed changes.)\n'
else
  step "4/4  A-G acceptance harness (heavy, ~11min — the transfer-plane gate)"
  if cargo test -p zeph-tests --test transfer_plane -- --ignored --test-threads=1; then
    ok "A-G harness"
  else
    bad "A-G harness"
  fi
fi

printf '\n'
if [[ $fail -eq 0 ]]; then
  printf '🟢 GATE PASSED — safe to roll\n'
else
  printf '🔴 GATE FAILED — do NOT roll\n'
fi
exit $fail
