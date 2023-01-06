#!/bin/bash
for i in {1..100}; do
  if RUST_LOG="curp,xline" ID="$i" cargo test --package curp --test server -- exe_exact_n_times --exact --nocapture >"scripts/logs/d-${i}.log"; then
    echo "pass exact_n round no.$i"
  else
    echo "fail exact_n round no.$i"
    exit 1
  fi
  # if RUST_LOG="curp,xline" ID="$i" cargo test --package xline --test kv_test -- test_kv_delete --exact --nocapture >"scripts/logs/d-${i}.log"; then
  #   echo "pass delete round no.$i"
  # else
  #   echo "fail delete round no.$i"
  #   exit 1
  # fi
done
