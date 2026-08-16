#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
core=$root/formal/recorder_retention_core.pml
transition=$root/formal/recorder_retention_transition.pml
case "$(spin -V 2>&1)" in *"Spin Version 6.5.2"*) ;; *) echo "need SPIN 6.5.2" >&2; exit 1 ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-spin.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
cd "$tmp"

run() {
  name=$1
  model=$2
  cflags=$3
  rm -f model.pml.trail pan.trail
  cp "$model" model.pml
  spin -a $cflags model.pml
  cc -O2 -DSAFETY $cflags -o pan pan.c
  ./pan -m100000 -w20 > "pan.$name.out"
  test ! -e model.pml.trail
  test ! -e pan.trail
  awk -v name="$name" '
    /states, stored/ { states=$1 }
    /depth reached/ { depth=$6; sub(/,/, "", depth) }
    /errors:/ { errors=$NF }
    /unreached in/ { unreached="present" }
    END {
      if (states == "" || depth == "" || errors != 0) exit 1
      printf "%s states=%s depth=%s errors=%s unreached_summary=%s\n", name, states, depth, errors, (unreached ? unreached : "none")
    }
  ' "pan.$name.out"
}

run safety_unseeded_por "$core" ''
run safety_unseeded_noreduce "$core" '-DNOREDUCE'
run safety_seeded_por "$core" '-DSAFETY_SEEDED'
run safety_seeded_noreduce "$core" '-DSAFETY_SEEDED -DNOREDUCE'
run safety_post_por "$core" '-DSAFETY_POST'
run safety_post_noreduce "$core" '-DSAFETY_POST -DNOREDUCE'
run witness_all "$core" '-DWITNESS_ALL'
run transition_unseeded_por "$transition" ''
run transition_unseeded_noreduce "$transition" '-DNOREDUCE'
run transition_post_stop_por "$transition" '-DPROFILE_POST_STOP'
run transition_post_stop_noreduce "$transition" '-DPROFILE_POST_STOP -DNOREDUCE'
run transition_post_install_por "$transition" '-DPROFILE_POST_INSTALL'
run transition_post_install_noreduce "$transition" '-DPROFILE_POST_INSTALL -DNOREDUCE'
run transition_post_activation_por "$transition" '-DPROFILE_POST_ACTIVATION'
run transition_post_activation_noreduce "$transition" '-DPROFILE_POST_ACTIVATION -DNOREDUCE'
run transition_witness_all "$transition" '-DWITNESS_ALL'
printf 'core=%s\ntransition=%s\n' "$core" "$transition"
