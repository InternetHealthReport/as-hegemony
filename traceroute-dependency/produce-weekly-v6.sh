#!/bin/bash
set -euo pipefail
HEGE_START_TS=$(date --utc +%Y-%m-%dT00:00:00)
HEGE_STOP_TS1=$(date --utc +%Y-%m-%dT00:00:01)
HEGE_STOP_TS2=$(date --utc +%Y-%m-%dT00:00:02)

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev6-topology-weekly.json -c traceroutev6_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev6-topology-weekly.json -c traceroutev6_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" -v 6 &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev6-topology-weekly.json -c traceroutev6_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev6-topology-rank-as-path-length-weekly.json -c traceroutev6_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev6-topology-rank-as-path-length-weekly.json -c traceroutev6_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" -v 6 &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev6-topology-rank-as-path-length-weekly.json -c traceroutev6_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev6-topology-rank-rtt-weekly.json -c traceroutev6_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev6-topology-rank-rtt-weekly.json -c traceroutev6_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" -v 6 &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev6-topology-rank-rtt-weekly.json -c traceroutev6_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers