# hw4_api.py - Assignment 4 Sharding API
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence, Dict, Any, Optional, Union

import requests

from .util import log

class _NodeLike(Protocol):
    name: str
    external_port: int
    ip: str
    index: int

@dataclass
class KvsClientException(Exception):
    message: str

class KvsTimeout(Exception):
    def __str__(self):
        return "request timed out"

class CreateClient(Protocol):
    def __call__(self, name: str) -> "KvsClient": ...

class KvsFixture:
    def __init__(self):
        self.clients: list[KvsClient] = []

    def create_client(self, name: str) -> "KvsClient":
        client = KvsClient(name=name)
        self.clients.append(client)
        return client

class KvsClient:
    def __init__(self, name: str, timeout: int = 10, num_retries: int = 3):
        self.name = name
        self.timeout = timeout
        self.num_retries = num_retries
        self.causal_metadata = {}
        self._log = []
        self._id = 0

    def _new_id(self) -> int:
        id = self._id
        self._id += 1
        return id

    def dump_logs(self, path: Path) -> None:
        """Dump the logs to a file"""
        path.mkdir(parents=True, exist_ok=True)
        with (path / f"{self.name}.jsonl").open("w") as f:
            for item in self._log:
                json.dump(item, f)
                f.write("\n")

    def _base_url(self, node: _NodeLike) -> str:
        return f"http://localhost:{node.external_port}"

    def _request(self, corr_id: int, node: _NodeLike, method: str, path: str, **kwargs) -> requests.Response:
        """Send HTTP request with causal metadata handling"""
        url = f"{self._base_url(node)}/{path.lstrip('/')}"
        response = None
        timed_out = False
        
        # Add causal metadata to request body if it exists
        if 'json' in kwargs and self.causal_metadata:
            kwargs['json']['causal-metadata'] = self.causal_metadata
        elif method != 'DELETE':
            if 'json' not in kwargs:
                kwargs['json'] = {}
            kwargs['json']['causal-metadata'] = self.causal_metadata

        # Add X-Causal-Metadata header (Assignment 4 requirement)
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['X-Causal-Metadata'] = json.dumps(self.causal_metadata)

        try:
            for i in range(self.num_retries):
                try:
                    response = getattr(requests, method)(url, timeout=self.timeout, **kwargs)
                    if response.status_code == 500:
                        return response
                    break
                except requests.exceptions.ConnectionError:
                    if i == self.num_retries - 1:
                        raise
                    time.sleep(0.5)
            
            if response is None:
                raise KvsClientException(f"failed to connect after {self.num_retries} attempts")

            # Update causal metadata from response if available
            if response.ok and response.text:
                try:
                    response_json = response.json()
                    if 'causal-metadata' in response_json:
                        self.causal_metadata = response_json['causal-metadata']
                except:
                    pass  # Response might not be JSON

            return response
            
        except requests.exceptions.Timeout:
            timed_out = True
            res = requests.Response()
            res.status_code = 408
            return res
        finally:
            log_entry = {
                "id": corr_id,
                "url": url,
                "method": method,
                "payload": kwargs.get("json"),
                "headers": kwargs.get("headers", {}),
                "status_code": response.status_code if response is not None else None,
                "response_text": response.text if response is not None else None,
                "timed_out": timed_out
            }
            self._log.append(log_entry)

    def ping(self, node: _NodeLike) -> bool:
        """Test if a node is responsive"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: ping")
        try:
            res = self._request(id, node, "get", "ping")
            return res.status_code == 200
        except Exception:
            return False

    def put(self, node: _NodeLike, key: str, value: str) -> Dict[str, Any]:
        """Put a key-value pair (with automatic sharding/proxying)"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: put {key!r} := {value!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")
        
        res = self._request(id, node, "put", f"data/{key}", json={"value": value})
        
        response_data = {
            "status_code": res.status_code,
            "ok": res.ok
        }
        
        if res.ok and res.text:
            try:
                response_json = res.json()
                response_data["causal_metadata"] = response_json.get("causal-metadata", {})
            except:
                response_data["causal_metadata"] = {}
        
        return response_data

    def get(self, node: _NodeLike, key: str) -> Dict[str, Any]:
        """Get a value (with automatic sharding/proxying)"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: get {key!r}")
        if len(key) == 0:
            raise ValueError("key cannot be empty")
        
        res = self._request(id, node, "get", f"data/{key}", json={})
        
        response_data = {
            "status_code": res.status_code,
            "ok": res.ok,
            "value": None
        }
        
        if res.ok and res.text:
            try:
                data = res.json()
                response_data["value"] = data.get("value")
                response_data["causal_metadata"] = data.get("causal-metadata", {})
                log(f"client {self.name} [{id}] -> {node.name}: get {key!r} |> {response_data['value']!r}")
            except:
                pass
        
        return response_data

    def get_all(self, node: _NodeLike) -> Dict[str, Any]:
        """Get all key-value pairs from this node's shard only"""
        id = self._new_id()
        log(f"client {self.name} [{id}] -> {node.name}: list (shard-local)")
        
        res = self._request(id, node, "get", "data", json={})
        
        response_data = {
            "status_code": res.status_code,
            "ok": res.ok,
            "values": {}
        }
        
        if res.ok and res.text:
            try:
                data = res.json()
                response_data["values"] = data.get("items", {})
                response_data["causal_metadata"] = data.get("causal-metadata", {})
                log(f"client {self.name} [{id}] -> {node.name}: list |> [{len(response_data['values'])} items]")
            except:
                pass
        
        return response_data

    def send_view(self, node: _NodeLike, view: Sequence[_NodeLike]) -> bool:
        """Send a legacy view update to a node"""
        id = self._new_id()
        view_ = [dict(address=f"{n.ip}:8081", id=n.index) for n in view]
        log(f"client {self.name} [{id}] -> {node.name}: legacy view {[f'{n.name} (addr={n.ip}:8081, id={n.index})' for n in view]}")
        
        res = self._request(id, node, "put", "view", json={"view": view_})
        return res.ok

    def send_sharded_view(self, node: _NodeLike, sharded_view: Dict[str, Sequence[_NodeLike]]) -> bool:
        """Send a sharded view update to a node"""
        id = self._new_id()
        
        # Convert to Assignment 4 sharded format
        view_obj = {}
        for shard_name, shard_nodes in sharded_view.items():
            view_obj[shard_name] = [
                {"address": f"{n.ip}:8081", "id": n.index} 
                for n in shard_nodes
            ]
        
        log(f"client {self.name} [{id}] -> {node.name}: sharded view {list(view_obj.keys())}")
        
        res = self._request(id, node, "put", "view", json={"view": view_obj})
        return res.ok

    def broadcast_view(self, nodes: Sequence[_NodeLike]) -> bool:
        """Broadcast a legacy view update to all nodes"""
        log(f"client {self.name}: broadcast legacy view")
        success = True
        for node in nodes:
            if not self.send_view(node, nodes):
                success = False
        return success

    def broadcast_sharded_view(self, sharded_view: Dict[str, Sequence[_NodeLike]]) -> bool:
        """Broadcast a sharded view to all nodes in the view"""
        log(f"client {self.name}: broadcast sharded view")
        success = True
        
        # Get all nodes across all shards
        all_nodes = []
        for shard_nodes in sharded_view.values():
            all_nodes.extend(shard_nodes)
        
        # Send sharded view to all nodes
        for node in all_nodes:
            if not self.send_sharded_view(node, sharded_view):
                success = False
        
        return success
    
    def reset_causal_metadata(self):
        """Reset the client's causal metadata to empty"""
        self.causal_metadata = {}

    def get_shard_data(self, nodes: Sequence[_NodeLike]) -> Dict[str, Dict[str, str]]:
        """Get data from each shard separately (for distribution analysis)"""
        shard_data = {}
        
        for i, node in enumerate(nodes):
            shard_name = f"shard_{i}"
            result = self.get_all(node)
            if result["ok"]:
                shard_data[shard_name] = result["values"]
            else:
                shard_data[shard_name] = {}
        
        return shard_data

    def verify_key_distribution(self, keys: Sequence[str], nodes: Sequence[_NodeLike]) -> Dict[str, Any]:
        """Verify that keys are properly distributed across shards"""
        shard_data = self.get_shard_data(nodes)
        
        # Count keys per shard
        distribution = {}
        total_keys = 0
        all_found_keys = set()
        
        for shard_name, shard_keys in shard_data.items():
            count = len(shard_keys)
            distribution[shard_name] = count
            total_keys += count
            all_found_keys.update(shard_keys.keys())
        
        # Check for missing or duplicated keys
        missing_keys = set(keys) - all_found_keys
        extra_keys = all_found_keys - set(keys)
        
        return {
            "distribution": distribution,
            "total_keys": total_keys,
            "missing_keys": list(missing_keys),
            "extra_keys": list(extra_keys),
            "shard_data": shard_data
        }

    def wait_for_convergence(self, keys: Sequence[str], nodes: Sequence[_NodeLike], 
                           max_wait: float = 10.0) -> bool:
        """Wait for eventual consistency across all shards"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                # Check if all keys are accessible from all nodes
                all_accessible = True
                for key in keys:
                    for node in nodes:
                        result = self.get(node, key)
                        if not result["ok"]:
                            all_accessible = False
                            break
                    if not all_accessible:
                        break
                
                if all_accessible:
                    return True
                
                time.sleep(0.5)
            except:
                time.sleep(0.5)
        
        return False

    def test_causal_consistency_across_shards(self, nodes: Sequence[_NodeLike]) -> bool:
        """Test that causal consistency is maintained across shard boundaries"""
        try:
            # Create a causal chain that might span shards
            self.put(nodes[0], "causal_test_1", "value_1")
            
            # Read from different node, creating dependency
            result = self.get(nodes[1] if len(nodes) > 1 else nodes[0], "causal_test_1")
            if not result["ok"]:
                return False
            
            # Write dependent value
            self.put(nodes[0], "causal_test_2", "value_2")
            
            # Verify causal order is maintained
            result = self.get(nodes[-1], "causal_test_2")
            if not result["ok"]:
                return False
            
            # Final verification - should be able to read both
            result1 = self.get(nodes[-1], "causal_test_1")
            result2 = self.get(nodes[-1], "causal_test_2")
            
            return result1["ok"] and result2["ok"]
            
        except Exception as e:
            log(f"Causal consistency test failed: {e}")
            return False

    def measure_proxy_latency(self, key: str, nodes: Sequence[_NodeLike]) -> Dict[str, float]:
        """Measure latency difference between local and proxied requests"""
        if len(nodes) < 2:
            return {}
        
        # Put a key and determine which shard it's in
        self.put(nodes[0], key, "test_value")
        time.sleep(1)
        
        # Find which node has the key locally
        local_node = None
        remote_nodes = []
        
        for node in nodes:
            result = self.get_all(node)
            if result["ok"] and key in result["values"]:
                local_node = node
            else:
                remote_nodes.append(node)
        
        if not local_node or not remote_nodes:
            return {}
        
        # Measure local access time
        start_time = time.time()
        for _ in range(5):
            self.get(local_node, key)
        local_time = (time.time() - start_time) / 5
        
        # Measure remote access time (proxied)
        start_time = time.time()
        for _ in range(5):
            self.get(remote_nodes[0], key)
        remote_time = (time.time() - start_time) / 5
        
        return {
            "local_avg_ms": local_time * 1000,
            "remote_avg_ms": remote_time * 1000,
            "proxy_overhead_ms": (remote_time - local_time) * 1000,
            "proxy_overhead_percent": ((remote_time - local_time) / local_time * 100) if local_time > 0 else 0
        }

# Utility functions for sharding tests
def create_sharded_view(shard_config: Dict[str, Sequence[_NodeLike]]) -> Dict[str, Sequence[_NodeLike]]:
    """Helper to create sharded view configuration"""
    return shard_config

def nodes_to_shard_view(nodes: Sequence[_NodeLike], num_shards: int = 2) -> Dict[str, Sequence[_NodeLike]]:
    """Convert node list to sharded view (helper for tests)"""
    sharded_view = {}
    nodes_per_shard = len(nodes) // num_shards
    
    for shard_idx in range(num_shards):
        start_idx = shard_idx * nodes_per_shard
        end_idx = start_idx + nodes_per_shard
        
        # For the last shard, include any remaining nodes
        if shard_idx == num_shards - 1:
            end_idx = len(nodes)
        
        shard_nodes = nodes[start_idx:end_idx]
        
        if shard_nodes:  # Only create shard if it has nodes
            shard_name = f"Shard{shard_idx + 1}"
            sharded_view[shard_name] = shard_nodes
    
    return sharded_view

def analyze_key_distribution(shard_data: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """Analyze key distribution across shards for load balancing"""
    total_keys = sum(len(keys) for keys in shard_data.values())
    
    if total_keys == 0:
        return {"total_keys": 0, "balance_score": 1.0, "distribution": {}}
    
    # Calculate distribution metrics
    distribution = {}
    max_keys = 0
    min_keys = float('inf')
    
    for shard_name, keys in shard_data.items():
        count = len(keys)
        distribution[shard_name] = count
        max_keys = max(max_keys, count)
        min_keys = min(min_keys, count)
    
    # Calculate balance score (1.0 = perfect balance, 0.0 = worst balance)
    if max_keys == 0:
        balance_score = 1.0
    else:
        balance_score = min_keys / max_keys
    
    # Calculate expected keys per shard
    num_shards = len(shard_data)
    expected_per_shard = total_keys / num_shards if num_shards > 0 else 0
    
    return {
        "total_keys": total_keys,
        "distribution": distribution,
        "balance_score": balance_score,
        "max_keys_per_shard": max_keys,
        "min_keys_per_shard": min_keys,
        "expected_per_shard": expected_per_shard,
        "imbalance_ratio": (max_keys - min_keys) / total_keys if total_keys > 0 else 0
    }