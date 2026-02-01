"""
election.py - Bully Algorithm for leader election

Implements the Bully Algorithm - highest ID wins.
Nodes with higher IDs have higher priority to be leader.

Basic flow:
1. Send ELECTION to nodes with higher IDs
2. If anyone responds, wait for them to become leader
3. If no one responds, we're the leader
4. New leader tells everyone via COORDINATOR message

Member 2 - Traffic Light System
"""

import socket
import threading
import time
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ElectionService:
    """
    Handles leader election using Bully Algorithm.
    Highest ID node becomes the leader.
    """
    
    def __init__(self, node_id, node_name, get_peers, on_leader_elected, send_message=None):
        """Set up election service"""
        self.node_id = node_id
        self.node_name = node_name
        self.get_peers = get_peers
        self.on_leader_elected = on_leader_elected
        self.send_message = send_message
        
        self._leader_id = None
        self._leader_name = None
        self._is_election_in_progress = False
        self._received_answer = False
        self._election_lock = threading.Lock()
        
        self.election_timeout = 5.0
        self.coordinator_timeout = 5.0
        
        # Reliable COORDINATOR delivery
        self._coordinator_ack_timeout = 0.5  # Time to wait for each ACK
        self._coordinator_max_retries = 5    # Max retries per peer
        self._pending_coordinator_acks = {}  # peer_id -> {'retries': n, 'peer_info': info}
        self._coordinator_ack_lock = threading.Lock()
        self._coordinator_retry_thread = None
        
        logger.info(f"Election service started for {node_name} (ID: {node_id})")
    
    @property
    def leader_id(self):
        """Get the current leader's ID (thread-safe)."""
        return self._leader_id
    
    @property
    def leader_name(self):
        """Get the current leader's name (thread-safe)."""
        return self._leader_name
    
    @property
    def is_leader(self):
        """Check if this node is the current leader."""
        return self._leader_id == self.node_id
    
    def start_election(self):
        """Start election - send ELECTION to higher ID nodes"""
        with self._election_lock:
            if self._is_election_in_progress:
                return
            
            self._is_election_in_progress = True
            self._received_answer = False
        
        logger.info(f"Node {self.node_name}: Starting election...")
        
        peers = self.get_peers()
        
        # Find nodes with higher IDs
        higher_id_peers = {
            peer_id: peer_info 
            for peer_id, peer_info in peers.items() 
            if peer_id > self.node_id
        }
        
        if not higher_id_peers:
            # No one has higher ID, we're the leader
            logger.info(f"Node {self.node_name}: No higher-ID nodes, becoming leader")
            self._become_leader()
            return
        
        # Send ELECTION to all higher-ID nodes
        for peer_id, peer_info in higher_id_peers.items():
            self._send_election_message(peer_id, peer_info)
        
        # Wait for answers
        timer_thread = threading.Thread(
            target=self._wait_for_answer,
            daemon=True
        )
        timer_thread.start()
    
    def _send_election_message(self, peer_id, peer_info):
        """Send ELECTION message to a peer"""
        if self.send_message:
            message = {
                'type': 'ELECTION',
                'sender_id': self.node_id,
                'sender_name': self.node_name,
                'payload': {}
            }
            try:
                self.send_message(peer_id, peer_info, message)
            except Exception as e:
                logger.error(f"Failed to send ELECTION to {peer_id}: {e}")
    
    def _wait_for_answer(self):
        """Wait for responses from higher-ID nodes"""
        time.sleep(self.election_timeout)
        
        with self._election_lock:
            if not self._received_answer:
                logger.info(f"Node {self.node_name}: No answer received, becoming leader")
                self._become_leader()
            else:
                logger.info(f"Node {self.node_name}: Got answer, waiting for coordinator")
                coordinator_timer = threading.Thread(
                    target=self._wait_for_coordinator,
                    daemon=True
                )
                coordinator_timer.start()
    
    def _wait_for_coordinator(self):
        """Wait for coordinator msg, restart if needed"""
        time.sleep(self.coordinator_timeout)
        
        with self._election_lock:
            if self._leader_id is None:
                logger.warning(f"Node {self.node_name}: No coordinator, restarting")
                self._is_election_in_progress = False
        
        if self._leader_id is None:
            self.start_election()
    
    def _become_leader(self):
        """Set ourselves as leader and tell everyone"""
        self._leader_id = self.node_id
        self._leader_name = self.node_name
        self._is_election_in_progress = False
        
        logger.info(f"*** Node {self.node_name} is now LEADER ***")
        
        self._broadcast_coordinator()
        
        if self.on_leader_elected:
            self.on_leader_elected(self.node_id, self.node_name)
    
    def _broadcast_coordinator(self):
        """Tell all nodes we're the leader with reliable delivery"""
        peers = self.get_peers()
        
        if not peers:
            logger.info(f"Node {self.node_name}: No peers to notify about leadership")
            return
        
        # Initialize pending ACKs for all peers
        with self._coordinator_ack_lock:
            self._pending_coordinator_acks.clear()
            for peer_id, peer_info in peers.items():
                self._pending_coordinator_acks[peer_id] = {
                    'retries': 0,
                    'peer_info': peer_info,
                    'last_sent': time.time()
                }
        
        # Send initial COORDINATOR to all peers
        self._send_coordinator_to_pending()
        
        # Start retry thread
        if self._coordinator_retry_thread is None or not self._coordinator_retry_thread.is_alive():
            self._coordinator_retry_thread = threading.Thread(
                target=self._coordinator_retry_loop,
                daemon=True
            )
            self._coordinator_retry_thread.start()
    
    def _send_coordinator_to_pending(self):
        """Send COORDINATOR message to all peers that haven't ACKed yet"""
        message = {
            'type': 'COORDINATOR',
            'sender_id': self.node_id,
            'sender_name': self.node_name,
            'payload': {
                'leader_id': self.node_id,
                'leader_name': self.node_name
            }
        }
        
        with self._coordinator_ack_lock:
            for peer_id, ack_info in list(self._pending_coordinator_acks.items()):
                if self.send_message:
                    try:
                        self.send_message(peer_id, ack_info['peer_info'], message)
                        ack_info['last_sent'] = time.time()
                        if ack_info['retries'] > 0:
                            logger.info(f"Node {self.node_name}: Retrying COORDINATOR to {peer_id} (retry #{ack_info['retries']})")
                    except Exception as e:
                        logger.error(f"Failed to send COORDINATOR to {peer_id}: {e}")
    
    def _coordinator_retry_loop(self):
        """Retry COORDINATOR messages until all ACKs received or max retries"""
        while self._leader_id == self.node_id:  # Only retry while we're still leader
            time.sleep(self._coordinator_ack_timeout)
            
            with self._coordinator_ack_lock:
                if not self._pending_coordinator_acks:
                    logger.info(f"Node {self.node_name}: All COORDINATOR ACKs received")
                    break
                
                # Check which peers need retry
                peers_to_remove = []
                for peer_id, ack_info in self._pending_coordinator_acks.items():
                    ack_info['retries'] += 1
                    
                    if ack_info['retries'] > self._coordinator_max_retries:
                        logger.warning(f"Node {self.node_name}: Gave up on COORDINATOR to {peer_id} after {self._coordinator_max_retries} retries")
                        peers_to_remove.append(peer_id)
                
                for peer_id in peers_to_remove:
                    del self._pending_coordinator_acks[peer_id]
                
                if not self._pending_coordinator_acks:
                    break
            
            # Send retries
            self._send_coordinator_to_pending()
    
    def handle_coordinator_ack(self, msg):
        """Handle COORDINATOR_ACK - peer acknowledged our leadership"""
        sender_id = msg.get('sender_id')
        sender_name = msg.get('sender_name', 'Unknown')
        
        with self._coordinator_ack_lock:
            if sender_id in self._pending_coordinator_acks:
                del self._pending_coordinator_acks[sender_id]
                logger.info(f"Node {self.node_name}: Got COORDINATOR_ACK from {sender_name}")
    
    def handle_election_message(self, msg):
        """Got ELECTION from someone - respond if we have higher ID"""
        sender_id = msg.get('sender_id')
        sender_name = msg.get('sender_name', 'Unknown')
        
        logger.info(f"Node {self.node_name}: Got ELECTION from {sender_name}")
        
        # Only respond if we have a HIGHER ID than the sender
        if self.node_id > sender_id:
            # Send ANSWER and start our own election
            self._send_answer_message(sender_id, msg.get('sender_info'))
            
            election_thread = threading.Thread(
                target=self.start_election,
                daemon=True
            )
            election_thread.start()
    
    def _send_answer_message(self, peer_id, peer_info):
        """Send ANSWER - we're alive and have higher priority"""
        if self.send_message:
            message = {
                'type': 'ANSWER',
                'sender_id': self.node_id,
                'sender_name': self.node_name,
                'payload': {}
            }
            
            if peer_info is None:
                peers = self.get_peers()
                peer_info = peers.get(peer_id)
            
            if peer_info:
                try:
                    self.send_message(peer_id, peer_info, message)
                except Exception as e:
                    logger.error(f"Failed to send ANSWER: {e}")
    
    def handle_answer_message(self, msg):
        """Got ANSWER - someone with higher ID is alive"""
        sender_id = msg.get('sender_id')
        sender_name = msg.get('sender_name', 'Unknown')
        
        logger.info(f"Node {self.node_name}: Got ANSWER from {sender_name}")
        
        with self._election_lock:
            self._received_answer = True
    
    def handle_coordinator_message(self, msg):
        """New leader announcement - only accept from higher-ID nodes"""
        sender_id = msg.get('sender_id')
        sender_name = msg.get('sender_name', 'Unknown')
        
        logger.info(f"Node {self.node_name}: Got COORDINATOR from {sender_name}")
        
        # Only accept COORDINATOR from nodes with higher or equal ID
        # This prevents lower-ID nodes from incorrectly claiming leadership
        if sender_id < self.node_id:
            logger.info(f"Node {self.node_name}: Ignoring COORDINATOR from {sender_name} (lower ID)")
            # We have higher ID, so we should be leader - start election
            if not self._is_election_in_progress:
                threading.Thread(target=self.start_election, daemon=True).start()
            return
        
        with self._election_lock:
            self._leader_id = sender_id
            self._leader_name = sender_name
            self._is_election_in_progress = False
            self._received_answer = False
        
        logger.info(f"Node {self.node_name}: Accepting {sender_name} as leader")
        
        # Send ACK back to new leader
        self._send_coordinator_ack(sender_id, msg.get('sender_info'))
        
        if self.on_leader_elected:
            self.on_leader_elected(sender_id, sender_name)
    
    def _send_coordinator_ack(self, leader_id, leader_info):
        """Send COORDINATOR_ACK to the new leader"""
        if leader_info is None:
            peers = self.get_peers()
            leader_info = peers.get(leader_id)
        
        if leader_info is None:
            logger.warning(f"Node {self.node_name}: Cannot send COORDINATOR_ACK - no info for leader {leader_id}")
            return
        
        if self.send_message:
            message = {
                'type': 'COORDINATOR_ACK',
                'sender_id': self.node_id,
                'sender_name': self.node_name,
                'payload': {
                    'leader_id': leader_id
                }
            }
            try:
                self.send_message(leader_id, leader_info, message)
                logger.info(f"Node {self.node_name}: Sent COORDINATOR_ACK to {leader_id}")
            except Exception as e:
                logger.error(f"Failed to send COORDINATOR_ACK: {e}")
    
    def on_peer_failed(self, failed_peer_id):
        """Peer died - if it was leader, start new election"""
        logger.warning(f"Node {self.node_name}: Peer {failed_peer_id} failed")
        
        if failed_peer_id == self._leader_id:
            logger.warning(f"Node {self.node_name}: Leader failed, starting election...")
            
            with self._election_lock:
                self._leader_id = None
                self._leader_name = None
                self._is_election_in_progress = False
            
            self.start_election()
    
    def reset(self):
        """Clear all election state"""
        with self._election_lock:
            self._leader_id = None
            self._leader_name = None
            self._is_election_in_progress = False
            self._received_answer = False
        
        logger.info(f"Node {self.node_name}: Election reset")