#!/bin/bash
set -euo pipefail
HEGE_START_TS=$(date --utc +%Y-%m-%dT00:00:00)
HEGE_STOP_TS1=$(date --utc +%Y-%m-%dT00:00:01)
HEGE_STOP_TS2=$(date --utc +%Y-%m-%dT00:00:02)

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev4-topology-weekly.json -c traceroutev4_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev4-topology-weekly.json -c traceroutev4_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev4-topology-weekly.json -c traceroutev4_topology_weekly -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev4-topology-rank-as-path-length-weekly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev4-topology-rank-as-path-length-weekly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev4-topology-rank-as-path-length-weekly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers

python3 produce_bgpatom.py -C traceroute-dependency/config-traceroutev4-topology-rank-rtt-weekly.json -c traceroutev4_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS2}" &&
python3 produce_bcscore.py -C traceroute-dependency/config-traceroutev4-topology-rank-rtt-weekly.json -c traceroutev4_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" &&
python3 produce_hege.py -C traceroute-dependency/config-traceroutev4-topology-rank-rtt-weekly.json -c traceroutev4_topology_weekly_rank_rtt -s "${HEGE_START_TS}" -e "${HEGE_STOP_TS1}" --sparse_peers

# For aggregates based on one month
START_TS=$(date --utc +%Y-%m-%d --date "-3 weeks")
START_TS_FMT="${START_TS}T00:00:00"
END_TS_BGPATOM="${START_TS}T00:00:02"
END_TS_BCSCORE="${START_TS}T00:00:01"
END_TS_HEGE="${START_TS}T00:00:01"
python3 produce_bgpatom.py -C traceroute-dependency/config-atlas-as-path-length-monthly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "$START_TS_FMT" -e "$END_TS_BGPATOM" &&
python3 produce_bcscore.py -C traceroute-dependency/config-atlas-as-path-length-monthly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "$START_TS_FMT" -e "$END_TS_BCSCORE" &&
python3 produce_hege.py -C traceroute-dependency/config-atlas-as-path-length-monthly.json -c traceroutev4_topology_weekly_rank_as_path_length -s "$START_TS_FMT" -e "$END_TS_HEGE" --sparse_peers