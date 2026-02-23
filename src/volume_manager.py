"""
Distributed block storage manager (Longhorn-inspired).
Manages volumes, replicas, snapshots, and backups across a distributed cluster.
"""

import sqlite3
import json
import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from enum import Enum
from datetime import datetime
from collections import defaultdict


class VolumeState(str, Enum):
    CREATING = "creating"
    ATTACHED = "attached"
    DETACHED = "detached"
    DELETING = "deleting"


class ReplicaState(str, Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    REBUILDING = "rebuilding"


class Frontend(str, Enum):
    BLOCKDEV = "blockdev"
    ISCSI = "iscsi"


class AccessMode(str, Enum):
    READ_WRITE_ONCE = "ReadWriteOnce"
    READ_ONLY_MANY = "ReadOnlyMany"
    READ_WRITE_MANY = "ReadWriteMany"


@dataclass
class Replica:
    """Volume replica on a node."""
    id: str
    volume_id: str
    node_id: str
    address: str
    state: ReplicaState = ReplicaState.RUNNING
    disk_path: str = "/var/lib/longhorn"
    size_gb: int = 100
    data_synced: int = 100
    rebuild_progress: int = 0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "volume_id": self.volume_id,
            "node_id": self.node_id,
            "address": self.address,
            "state": self.state.value,
            "disk_path": self.disk_path,
            "size_gb": self.size_gb,
            "data_synced": self.data_synced,
            "rebuild_progress": self.rebuild_progress,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Snapshot:
    """Volume snapshot."""
    id: str
    volume_id: str
    name: str
    size_gb: int
    created_at: datetime = field(default_factory=datetime.now)
    labels: Dict[str, str] = field(default_factory=dict)
    parent_id: Optional[str] = None
    children: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "volume_id": self.volume_id,
            "name": self.name,
            "size_gb": self.size_gb,
            "created_at": self.created_at.isoformat(),
            "labels": self.labels,
            "parent_id": self.parent_id,
            "children": self.children,
        }


@dataclass
class Volume:
    """Distributed block storage volume."""
    id: str
    name: str
    size_gb: int
    replicas: int = 3
    state: VolumeState = VolumeState.DETACHED
    attached_to: Optional[str] = None
    frontend: Frontend = Frontend.BLOCKDEV
    access_mode: AccessMode = AccessMode.READ_WRITE_ONCE
    data_locality: str = "best-effort"
    node_selector: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    last_backup_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "size_gb": self.size_gb,
            "replicas": self.replicas,
            "state": self.state.value,
            "attached_to": self.attached_to,
            "frontend": self.frontend.value,
            "access_mode": self.access_mode.value,
            "data_locality": self.data_locality,
            "node_selector": self.node_selector,
            "created_at": self.created_at.isoformat(),
            "last_backup_at": self.last_backup_at.isoformat() if self.last_backup_at else None,
        }


class VolumeManager:
    """Manages distributed block storage volumes."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize VolumeManager with SQLite database."""
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/longhorn.db")
        
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS volumes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS replicas (
                    id TEXT PRIMARY KEY,
                    volume_id TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP,
                    FOREIGN KEY(volume_id) REFERENCES volumes(id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY,
                    volume_id TEXT NOT NULL,
                    config TEXT NOT NULL,
                    created_at TIMESTAMP,
                    FOREIGN KEY(volume_id) REFERENCES volumes(id)
                )
            """)
            conn.commit()

    def create_volume(self, name: str, size_gb: int, replicas: int = 3,
                     node_selector: Dict[str, str] = None, 
                     data_locality: str = "best-effort") -> str:
        """Create a new volume."""
        import uuid
        
        volume_id = f"volume_{uuid.uuid4().hex[:8]}"
        
        if node_selector is None:
            node_selector = {}
        
        volume = Volume(
            id=volume_id,
            name=name,
            size_gb=size_gb,
            replicas=replicas,
            state=VolumeState.CREATING,
            data_locality=data_locality,
            node_selector=node_selector,
        )
        
        config_json = json.dumps(volume.to_dict())
        now = datetime.now().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO volumes (id, name, config, created_at) VALUES (?, ?, ?, ?)",
                (volume_id, name, config_json, now)
            )
            
            # Create replicas
            for i in range(replicas):
                replica_id = f"replica_{uuid.uuid4().hex[:8]}"
                node_id = f"node_{i}"
                
                replica = Replica(
                    id=replica_id,
                    volume_id=volume_id,
                    node_id=node_id,
                    address=f"172.16.{i}.100",
                    size_gb=size_gb,
                )
                
                replica_json = json.dumps(replica.to_dict())
                conn.execute(
                    "INSERT INTO replicas (id, volume_id, config, created_at) VALUES (?, ?, ?, ?)",
                    (replica_id, volume_id, replica_json, now)
                )
            
            conn.commit()
        
        return volume_id

    def attach_volume(self, volume_id: str, node_id: str) -> bool:
        """Attach a volume to a node."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes WHERE id = ?", (volume_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            vol_dict = json.loads(row[0])
            vol_dict["state"] = "attached"
            vol_dict["attached_to"] = node_id
            
            conn.execute(
                "UPDATE volumes SET config = ? WHERE id = ?",
                (json.dumps(vol_dict), volume_id)
            )
            conn.commit()
        return True

    def detach_volume(self, volume_id: str) -> bool:
        """Detach a volume from its node."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes WHERE id = ?", (volume_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            vol_dict = json.loads(row[0])
            vol_dict["state"] = "detached"
            vol_dict["attached_to"] = None
            
            conn.execute(
                "UPDATE volumes SET config = ? WHERE id = ?",
                (json.dumps(vol_dict), volume_id)
            )
            conn.commit()
        return True

    def delete_volume(self, volume_id: str) -> bool:
        """Delete a volume."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes WHERE id = ?", (volume_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            vol_dict = json.loads(row[0])
            vol_dict["state"] = "deleting"
            
            conn.execute(
                "UPDATE volumes SET config = ? WHERE id = ?",
                (json.dumps(vol_dict), volume_id)
            )
            conn.execute("DELETE FROM replicas WHERE volume_id = ?", (volume_id,))
            conn.execute("DELETE FROM snapshots WHERE volume_id = ?", (volume_id,))
            conn.execute("DELETE FROM volumes WHERE id = ?", (volume_id,))
            conn.commit()
        return True

    def create_snapshot(self, volume_id: str, name: str, labels: Dict[str, str] = None) -> str:
        """Create a snapshot of a volume."""
        import uuid
        
        if labels is None:
            labels = {}
        
        snapshot_id = f"snapshot_{uuid.uuid4().hex[:8]}"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes WHERE id = ?", (volume_id,))
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Volume {volume_id} not found")
            
            vol_dict = json.loads(row[0])
            
            snapshot = Snapshot(
                id=snapshot_id,
                volume_id=volume_id,
                name=name,
                size_gb=vol_dict["size_gb"],
                labels=labels,
            )
            
            config_json = json.dumps(snapshot.to_dict())
            now = datetime.now().isoformat()
            
            conn.execute(
                "INSERT INTO snapshots (id, volume_id, config, created_at) VALUES (?, ?, ?, ?)",
                (snapshot_id, volume_id, config_json, now)
            )
            conn.commit()
        
        return snapshot_id

    def restore_snapshot(self, snapshot_id: str, target_volume_name: str) -> str:
        """Restore a snapshot to a new volume."""
        import uuid
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM snapshots WHERE id = ?", (snapshot_id,))
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Snapshot {snapshot_id} not found")
            
            snap_dict = json.loads(row[0])
            
            # Create new volume from snapshot
            new_volume_id = self.create_volume(
                name=target_volume_name,
                size_gb=snap_dict["size_gb"],
                replicas=3,
            )
        
        return new_volume_id

    def create_backup(self, volume_id: str, backup_target: str = "s3://") -> dict:
        """Create incremental backup of a volume."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes WHERE id = ?", (volume_id,))
            row = cursor.fetchone()
            if not row:
                return {"success": False, "error": "Volume not found"}
            
            vol_dict = json.loads(row[0])
            vol_dict["last_backup_at"] = datetime.now().isoformat()
            
            conn.execute(
                "UPDATE volumes SET config = ? WHERE id = ?",
                (json.dumps(vol_dict), volume_id)
            )
            conn.commit()
        
        return {
            "success": True,
            "volume_id": volume_id,
            "backup_target": backup_target,
            "backup_id": f"backup_{datetime.now().timestamp()}",
        }

    def rebuild_replica(self, replica_id: str) -> bool:
        """Mark replica as rebuilding."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM replicas WHERE id = ?", (replica_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            replica_dict = json.loads(row[0])
            replica_dict["state"] = "rebuilding"
            replica_dict["rebuild_progress"] = 0
            
            conn.execute(
                "UPDATE replicas SET config = ? WHERE id = ?",
                (json.dumps(replica_dict), replica_id)
            )
            conn.commit()
        return True

    def get_volume_status(self, volume_id: str) -> str:
        """Get volume health status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM replicas WHERE volume_id = ?", (volume_id,))
            
            running = 0
            error = 0
            total = 0
            
            for (config,) in cursor.fetchall():
                replica_dict = json.loads(config)
                total += 1
                if replica_dict["state"] == "running":
                    running += 1
                elif replica_dict["state"] == "error":
                    error += 1
            
            if total == 0:
                return "unknown"
            elif error > 0:
                return "faulted"
            elif running < total:
                return "degraded"
            else:
                return "healthy"

    def node_eviction(self, node_id: str) -> dict:
        """Handle node eviction."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT id, config FROM replicas WHERE json_extract(config, '$.node_id') = ?",
                (node_id,)
            )
            
            evicted = []
            for replica_id, config in cursor.fetchall():
                replica_dict = json.loads(config)
                replica_dict["state"] = "error"
                
                conn.execute(
                    "UPDATE replicas SET config = ? WHERE id = ?",
                    (json.dumps(replica_dict), replica_id)
                )
                
                # Start rebuild on other replicas
                vol_id = replica_dict["volume_id"]
                other_cursor = conn.execute(
                    "SELECT id FROM replicas WHERE volume_id = ? AND id != ?",
                    (vol_id, replica_id)
                )
                for (other_id,) in other_cursor.fetchall():
                    self.rebuild_replica(other_id)
                
                evicted.append(replica_id)
            
            conn.commit()
        
        return {
            "node_id": node_id,
            "evicted_replicas": evicted,
            "rebuild_started": len(evicted) > 0,
        }

    def get_dashboard(self) -> dict:
        """Get dashboard with storage utilization."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM volumes")
            
            volumes = [json.loads(config) for config, in cursor.fetchall()]
            
            healthy = sum(1 for v in volumes if self.get_volume_status(v["id"]) == "healthy")
            degraded = sum(1 for v in volumes if self.get_volume_status(v["id"]) == "degraded")
            faulted = sum(1 for v in volumes if self.get_volume_status(v["id"]) == "faulted")
            
            total_size = sum(v["size_gb"] for v in volumes)
            attached = sum(1 for v in volumes if v["state"] == "attached")
            
            return {
                "total_volumes": len(volumes),
                "attached": attached,
                "total_size_gb": total_size,
                "health": {
                    "healthy": healthy,
                    "degraded": degraded,
                    "faulted": faulted,
                },
            }

    def get_replica_balance(self) -> dict:
        """Get replica distribution across nodes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT config FROM replicas")
            
            balance = defaultdict(int)
            for (config,) in cursor.fetchall():
                replica_dict = json.loads(config)
                balance[replica_dict["node_id"]] += 1
            
            return dict(balance)

    def list_volumes(self) -> List[dict]:
        """List all volumes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id, name, created_at FROM volumes")
            return [{"id": row[0], "name": row[1], "created_at": row[2]} for row in cursor.fetchall()]


if __name__ == "__main__":
    import sys
    
    manager = VolumeManager()
    
    if len(sys.argv) < 2:
        print("Usage: volume_manager.py [volumes|create|snapshot|dashboard|balance]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "volumes":
        volumes = manager.list_volumes()
        for v in volumes:
            print(f"  {v['id']}: {v['name']}")
    elif cmd == "dashboard":
        dashboard = manager.get_dashboard()
        print(json.dumps(dashboard, indent=2))
    elif cmd == "balance":
        balance = manager.get_replica_balance()
        for node, count in balance.items():
            print(f"  {node}: {count} replicas")
    elif cmd == "create" and len(sys.argv) >= 4:
        name = sys.argv[2]
        size = int(sys.argv[3])
        replicas = int(sys.argv[5]) if len(sys.argv) > 4 and sys.argv[4] == "--replicas" else 3
        vol_id = manager.create_volume(name, size, replicas)
        print(f"Created volume: {vol_id}")
    elif cmd == "snapshot" and len(sys.argv) >= 4:
        volume_id = sys.argv[2]
        name = sys.argv[3]
        snap_id = manager.create_snapshot(volume_id, name)
        print(f"Created snapshot: {snap_id}")
