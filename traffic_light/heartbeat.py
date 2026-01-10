"""
heartbeat.py - Failure Detection and Peer Monitoring.

Tracks active peers and detects failures based on heartbeat/discovery timeouts.
"""

import threading
import time
import logging
from . import config

logger = logging.getLogger(__name__)

class HeartbeatMonitor:
    def __init__(self, node_id):
        """
        Initialize Heartbeat Monitor.
        
        Args:
            node_id (str): ID of the local node.
        """
        self.node_id = node_id
        self._peers = {}  # {node_id: {'ip': ..., 'port': ..., 'last_seen': ...}}
        self._lock = threading.Lock()
        self._running = False
        
        # Callbacks
        self.on_node_joined = None
        self.on_node_left = None

    def start(self):
        """Start the failure detection thread."""
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Heartbeat monitor started")

    def stop(self):
        """Stop the monitor."""
        self._running = False
        logger.info("Heartbeat monitor stopped")

    def update_peer(self, node_id, info):
        """
        Update the last seen timestamp for a peer.
        Called by Discovery or when a message is received.
        """
        with self._lock:
            current_time = time.time()
            if node_id not in self._peers:
                logger.info(f"New node discovered: {node_id}")
                info['last_seen'] = current_time
                self._peers[node_id] = info
                if self.on_node_joined:
                    self.on_node_joined(node_id, info)
            else:
                self._peers[node_id].update(info)
                self._peers[node_id]['last_seen'] = current_time

    def get_peers(self):
        """Return a copy of active peers."""
        with self._lock:
            return self._peers.copy()

    def _monitor_loop(self):
        """Periodically check for failed nodes."""
        while self._running:
            time.sleep(config.HEARTBEAT_INTERVAL)
            
            current_time = time.time()
            failed_nodes = []
            
            with self._lock:
                for node_id, info in list(self._peers.items()):
                    if current_time - info['last_seen'] > config.HEARTBEAT_TIMEOUT:
                        logger.warning(f"Node {node_id} timed out (last seen {current_time - info['last_seen']:.1f}s ago)")
                        failed_nodes.append(node_id)
                        del self._peers[node_id]
            
            # Notify callbacks (outside lock to avoid deadlocks)
            for node_id in failed_nodes:
                if self.on_node_left:
                    self.on_node_left(node_id)
