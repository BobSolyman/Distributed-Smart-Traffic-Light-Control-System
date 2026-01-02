"""
sequencer.py - Reliable Ordered Multicast

Implements reliable multicast with sequence numbers.
Leader assigns sequence numbers to ensure all nodes get updates in same order.

Each message gets a unique, incrementing sequence number.
Receivers send ACK back to sender.
Sender retries if no ACK within timeout.

Important: All traffic lights must agree on which light is GREEN/YELLOW/RED.
If messages arrive out of order or get lost, could have multiple GREEN lights (bad!).

Member 2 - Traffic Light System
"""

import socket
import threading
import time
import logging
from collections import defaultdict

# Set up logging for debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Sequencer:
    """
    Handles reliable multicast for traffic light updates.
    
    Leader sends phase updates with sequence numbers.
    Followers send ACKs back to confirm receipt.
    """
    
    def __init__(self, node_id, node_name, get_peers, send_message=None, on_phase_update=None):
        """Set up sequencer"""
        self.node_id = node_id
        self.node_name = node_name
        self.get_peers = get_peers
        self.send_message = send_message
        self.on_phase_update = on_phase_update
        
        # Leader state
        self._sequence_num = 0
        self._pending_acks = {}
        self._seq_lock = threading.Lock()
        
        # Follower state
        self._last_received_seq = 0
        self._message_buffer = {}
        self._recv_lock = threading.Lock()
        
        # Config
        self.ack_timeout = 3.0
        self.max_retries = 3
        self.retry_interval = 1.0
        self._running = False
        
        logger.info(f"Sequencer initialized for {node_name} (ID: {node_id})")
    
    def start(self):
        """Start the sequencer service"""
        self._running = True
        
        self._retry_thread = threading.Thread(
            target=self._retry_monitor_loop,
            daemon=True
        )
        self._retry_thread.start()
        
        logger.info(f"Sequencer started for {self.node_name}")
    
    def stop(self):
        """Stop the sequencer"""
        self._running = False
        logger.info(f"Sequencer stopped for {self.node_name}")
    
    # ============================================
    # LEADER Methods (Broadcasting Updates)
    # ============================================
    
    def broadcast_phase_update(self, current_green_node, all_node_phases):
        """
        Leader sends phase update to all nodes
        
        Returns the sequence number for this update
        """
        peers = self.get_peers()
        
        if not peers:
            logger.warning(f"Node {self.node_name}: No peers to send to")
            return 0
        
        with self._seq_lock:
            self._sequence_num += 1
            seq_num = self._sequence_num
            
            message = {
                'type': 'PHASE_UPDATE',
                'sender_id': self.node_id,
                'sender_name': self.node_name,
                'payload': {
                    'sequence_num': seq_num,
                    'current_green_node': current_green_node,
                    'node_phases': all_node_phases,
                    'timestamp': time.time()
                }
            }
            
            self._pending_acks[seq_num] = {
                'message': message,
                'pending_acks': set(peers.keys()),
                'retries': 0,
                'timestamp': time.time()
            }
        
        logger.info(f"Leader {self.node_name}: Broadcasting PHASE_UPDATE (seq={seq_num}, green={current_green_node})")
        
        for peer_id, peer_info in peers.items():
            self._send_phase_update(peer_id, peer_info, message)
        
        return seq_num
    
    def _send_phase_update(self, peer_id, peer_info, message):
        """Send phase update to a peer"""
        if self.send_message:
            try:
                self.send_message(peer_id, peer_info, message)
            except Exception as e:
                logger.error(f"Failed to send PHASE_UPDATE to {peer_id}: {e}")
    
    def _retry_monitor_loop(self):
        """Background thread to monitor and retry missing ACKs"""
        while self._running:
            time.sleep(self.retry_interval)
            
            current_time = time.time()
            messages_to_retry = []
            
            with self._seq_lock:
                for seq_num, ack_info in list(self._pending_acks.items()):
                    if not ack_info['pending_acks']:
                        del self._pending_acks[seq_num]
                        continue
                    
                    elapsed = current_time - ack_info['timestamp']
                    if elapsed > self.ack_timeout:
                        if ack_info['retries'] < self.max_retries:
                            messages_to_retry.append((seq_num, ack_info))
                            ack_info['retries'] += 1
                            ack_info['timestamp'] = current_time
                        else:
                            logger.warning(f"Gave up on seq={seq_num}, missing ACKs from: {ack_info['pending_acks']}")
                            del self._pending_acks[seq_num]
            
            for seq_num, ack_info in messages_to_retry:
                logger.info(f"Retrying PHASE_UPDATE seq={seq_num} (retry #{ack_info['retries']})")
                peers = self.get_peers()
                
                for peer_id in ack_info['pending_acks']:
                    if peer_id in peers:
                        self._send_phase_update(peer_id, peers[peer_id], ack_info['message'])
    
    def handle_ack(self, msg):
        """Got ACK from follower - mark it as received"""
        sender_id = msg.get('sender_id')
        seq_num = msg.get('payload', {}).get('sequence_num')
        
        logger.debug(f"Leader {self.node_name}: Got ACK for seq={seq_num} from node {sender_id}")
        
        with self._seq_lock:
            if seq_num in self._pending_acks:
                self._pending_acks[seq_num]['pending_acks'].discard(sender_id)
                
                if not self._pending_acks[seq_num]['pending_acks']:
                    logger.info(f"Leader {self.node_name}: All ACKs received for seq={seq_num}")
                    del self._pending_acks[seq_num]
    
    # ============================================
    # FOLLOWER Methods (Receiving Updates)
    # ============================================
    
    def handle_phase_update(self, msg, leader_id=None, leader_info=None):
        """
        Got PHASE_UPDATE from leader - send ACK and process if in order
        """
        sender_id = msg.get('sender_id')
        sender_name = msg.get('sender_name', 'Unknown')
        payload = msg.get('payload', {})
        
        seq_num = payload.get('sequence_num')
        current_green = payload.get('current_green_node')
        node_phases = payload.get('node_phases', {})
        
        logger.info(f"Node {self.node_name}: Got PHASE_UPDATE (seq={seq_num}, green={current_green})")
        
        # Send ACK immediately
        self._send_ack(sender_id, leader_info, seq_num)
        
        with self._recv_lock:
            expected_seq = self._last_received_seq + 1
            
            if seq_num == expected_seq:
                # This is the next message - process it
                self._process_phase_update(seq_num, current_green, node_phases)
                self._process_buffered_messages()
                
            elif seq_num > expected_seq:
                # Out of order - buffer it
                logger.warning(f"Node {self.node_name}: Out of order! Expected {expected_seq}, got {seq_num}")
                self._message_buffer[seq_num] = {
                    'current_green': current_green,
                    'node_phases': node_phases
                }
                
            else:
                # Old message - already processed
                logger.debug(f"Node {self.node_name}: Ignoring old seq={seq_num}")
    
    def _process_phase_update(self, seq_num, current_green, node_phases):
        """Process the phase update and call callback"""
        self._last_received_seq = seq_num
        
        logger.info(f"Node {self.node_name}: Processing seq={seq_num}, green={current_green}")
        
        if self.on_phase_update:
            self.on_phase_update(current_green, node_phases)
    
    def _process_buffered_messages(self):
        """Process any buffered out-of-order messages that are now in sequence"""
        while True:
            next_seq = self._last_received_seq + 1
            
            if next_seq in self._message_buffer:
                buffered_msg = self._message_buffer.pop(next_seq)
                self._process_phase_update(
                    next_seq,
                    buffered_msg['current_green'],
                    buffered_msg['node_phases']
                )
            else:
                break
    
    def _send_ack(self, leader_id, leader_info, seq_num):
        """Send ACK back to leader"""
        if self.send_message:
            message = {
                'type': 'ACK',
                'sender_id': self.node_id,
                'sender_name': self.node_name,
                'payload': {
                    'sequence_num': seq_num
                }
            }
            
            if leader_info is None:
                peers = self.get_peers()
                leader_info = peers.get(leader_id)
            
            if leader_info:
                try:
                    self.send_message(leader_id, leader_info, message)
                except Exception as e:
                    logger.error(f"Failed to send ACK: {e}")
    
    # ============================================
    # State Synchronization (for rejoining nodes)
    # ============================================
    
    def get_current_sequence_num(self):
        """Get the current sequence number"""
        with self._seq_lock:
            return self._sequence_num
    
    def sync_sequence_num(self, seq_num):
        """Sync sequence number when rejoining"""
        with self._seq_lock:
            self._sequence_num = seq_num
        
        with self._recv_lock:
            self._last_received_seq = seq_num
        
        logger.info(f"Node {self.node_name}: Synced sequence number to {seq_num}")
    
    def reset(self):
        """Clear all sequencer state"""
        with self._seq_lock:
            self._sequence_num = 0
            self._pending_acks.clear()
        
        with self._recv_lock:
            self._last_received_seq = 0
            self._message_buffer.clear()
        
        logger.info(f"Node {self.node_name}: Sequencer reset")
    
    def get_state_for_sync(self):
        """Get current state for synchronization"""
        with self._seq_lock:
            return {
                'sequence_num': self._sequence_num,
                'timestamp': time.time()
            }
    
    def apply_sync_state(self, state):
        """Apply sync state from leader"""
        if 'sequence_num' in state:
            self.sync_sequence_num(state['sequence_num'])
            logger.info(f"Node {self.node_name}: Applied sync state, seq={state['sequence_num']}")