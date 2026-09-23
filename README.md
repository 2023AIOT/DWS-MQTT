# DWS-MQTT: A High-Performance Distributed MQTT Communication Method for Industrial Internet of Things

This repository contains the implementation and experimental driver used to
compare DWS-MQTT with a centralized MQTT baseline and three distributed MQTT
baselines.

## Evaluated methods

| Label used in the paper | Python source | Role |
| --- | --- | --- |
| MQTT | `RUN_EXPERIMENTS.py` | Single-broker centralized baseline |
| TD-MQTT | `TD_MQTT.py` | Distributed baseline |
| TopoMQTT | `TopoMQTT.py` | Topology-aware distributed baseline |
| DMQTT | `DMQTT.py` | Distributed shortest-path-tree baseline |
| DWS-MQTT | `DWS_MQTT.py` | Proposed method |

Hyphens are used in paper labels, while importable Python module names use
underscores. `TopoMQTT.py` retains the name used by the original implementation.
`ARPS.py` is a standalone historical implementation; the main comparison does
not import it because the evaluated ARPS logic is included in `DWS_MQTT.py`.

## Repository files

The five-method comparison requires these files in the same directory:

```text
RUN_EXPERIMENTS.py
TD_MQTT.py
TopoMQTT.py
DMQTT.py
DWS_MQTT.py
requirements.txt
```

`RUN_EXPERIMENTS.py` is the only entry point for the reported comparison.

## Reference environment

The checked-in experiment was developed and verified with the following setup:

| Component | Reference value |
| --- | --- |
| Operating system | Windows, version `10.0.26200`, AMD64 |
| Python | CPython 3.9.13, 64 bit |
| MQTT broker | Eclipse Mosquitto 2.0.20 |
| MQTT client library | Eclipse Paho MQTT 2.1.0 |
| CPU | Intel Core i5-9300H, 4 physical cores / 8 logical processors |
| Memory | 15.92 GiB physical memory |

The experiment driver uses Windows process-management commands (`tasklist` and
`taskkill`). The current release should therefore be run on Windows. Mosquitto
must either be available on `PATH` or installed at one of:

```text
C:\Program Files\mosquitto\mosquitto.exe
C:\mosquitto\mosquitto.exe
```

## Installation

From the code repository, create a Python 3.9 environment and install the pinned dependencies:

```powershell
py -3.9 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import numpy, matplotlib, networkx, paho.mqtt.client, psutil, scipy; print('dependencies OK')"
```

Verify the broker separately:

```powershell
mosquitto -h
```

## Physical deployment and isolation

All broker instances in the reference experiment run as separate Mosquitto
processes on one physical host. They are not distributed across physical
machines, virtual machines, or containers. No CPU affinity, per-process CPU
quota, per-process memory quota, or network-namespace isolation is applied.
All processes share the host CPU, memory, loopback interface, and operating
system scheduler.

The centralized MQTT baseline uses a single broker listening on localhost:1883. The distributed methods use 30 independent broker processes, whose listeners are assigned consecutive loopback ports starting from 1884.

## Topology and baseline configuration


The experiment driver uses the following topology and baseline configurations:

| Method | Topology input used by the experiment driver |
| --- | --- |
| TD-MQTT | Neighbor topology built by `TD_MQTT.py` |
| TopoMQTT | All broker RTT and resource inputs initialized to `1.0`; overlay tree built by `TopoMQTT.py` |
| DMQTT | Complete NetworkX graph with 1 ms edge weights; broker `1884` is the root; Dijkstra-based tree |
| DWS-MQTT | Complete graph with 1 ms edge weights; ARPS root selection followed by the hierarchical tree |

DWS-MQTT uses initial ARPS weights `alpha=0.229`, `beta=0.175`,
`gamma=0.171`, `delta=0.250`, and `lambda=0.175`, a load threshold of `0.7`,
and an update interval of 30 s. The corresponding source files provide the remaining implementation-level parameters used by the experiment driver.

This localhost setup does not reproduce the heterogeneous latency,
bandwidth, congestion, jitter, packet loss, hardware variation, or failure
domains of a deployed industrial IoT network. Results from this setup primarily
characterize protocol and topology behavior under controlled single-host
conditions and should not be interpreted as direct wide-area deployment
measurements.

## Workload

| Parameter | Value |
| --- | --- |
| Distributed brokers | 30 |
| Centralized MQTT brokers | 1 |
| Publisher/subscriber pairs | 5 per broker |
| Distributed client pairs | 150 total at 30 brokers |
| Payload | 4096 bytes |
| QoS levels | 0 and 1 |
| System-wide publish rates | 100, 300, 500, 800, 1000 messages/s |
| Warm-up per run | 10 s |
| Measurement interval per run | 50 s |
| Repetitions | One at each rate; 10 at 1000 messages/s for 95% confidence intervals |


The message rate is a system-wide target and is divided among active publisher
clients. Topic-overlap and wildcard modes replace a configured fraction of
these ordinary subscriptions as described below.

## Experiment commands

Run commands from the repository root. Close unrelated Mosquitto instances
first: the driver terminates running `mosquitto.exe` processes during cleanup.

```powershell
# Main five-method comparison, 30 distributed brokers
python RUN_EXPERIMENTS.py default

# Broker-count scaling at 10, 20, and 30 brokers
python RUN_EXPERIMENTS.py scaled

# 10-broker subscription churn: 30% of subscribers every 10 s
python RUN_EXPERIMENTS.py churn

# 10-broker topic-overlap experiment: 30% overlap, changed every 15 s
python RUN_EXPERIMENTS.py overlap

# 10-broker wildcard experiment: 20% wildcard subscriptions, changed every 20 s
python RUN_EXPERIMENTS.py wildcard

# Run all experiment modes sequentially
python RUN_EXPERIMENTS.py full
```

The optional second positional argument is accepted as a displayed duration,
but the reported comparison is governed by the fixed 10 s warm-up and 50 s
measurement intervals. Use the commands above for the reference experiment.

## Outputs

At the start of the default or full run, the driver writes:

```text
experiment_results/reproducibility_manifest.json
```

The manifest records the platform, Python and package versions, workload and broker settings. 
Generated Mosquitto configurations and logs are written below:

```text
experiment_results/mosquitto_configs/
```



