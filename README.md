# BlackRoad OS Longhorn Volume Manager

Distributed block storage manager with Longhorn-inspired architecture.

## Features
- Distributed volume management across nodes
- Multi-replica synchronization and rebuilding
- Snapshot and backup capabilities
- Node eviction and replica rebalancing
- Health monitoring and status tracking
- Data locality optimization

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
python src/volume_manager.py volumes
python src/volume_manager.py create worlds-data 100 --replicas 3
python src/volume_manager.py snapshot VOLUME_ID daily-backup
python src/volume_manager.py dashboard
```
