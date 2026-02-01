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

import threading
import time
import logging
from . import message
from . import config

logger = logging.getLogger(__name__)


class Sequencer:
    def __init__(self, node_id, node_name, get_peers, send_message, on_phase_update):
        self.node_id = node_id
        self.node_name = node_name
        self.get_peers = get_peers
        self.send_message = send_message
        self.on_phase_update = on_phase_update

        # Leader state
        self._sequence_num = 0
        # seq -> {"msg": msg, "missing": set(peer_ids), "last_sent": float, "retries": int}
        self._pending = {}
        self._pending_lock = threading.Lock()

        # Message history for NACK recovery (leader keeps recent messages)
        self._message_history = {}  # seq -> msg (for resending on NACK)
        self._history_lock = threading.Lock()
        self._max_history_size = 100  # Keep last 100 messages for recovery

        # Follower state
        self._last_received_seq = 0
        self._message_buffer = {}  # seq -> msg
        self._buffer_lock = threading.Lock()
        
        # NACK state - track which seqs we've requested to avoid spam
        self._pending_nacks = set()  # seq numbers we've sent NACKs for
        self._nack_lock = threading.Lock()

        self._running = False
        self._resend_thread = None
        self._nack_retry_thread = None

        logger.info(f"Sequencer initialized for {node_name} (ID: {node_id})")

    def start(self):
        self._running = True
        self._resend_thread = threading.Thread(target=self._resend_monitor_loop, daemon=True)
        self._resend_thread.start()
        self._nack_retry_thread = threading.Thread(target=self._nack_retry_loop, daemon=True)
        self._nack_retry_thread.start()
        logger.info(f"Sequencer started for {self.node_name}")

    def stop(self):
        self._running = False
        logger.info(f"Sequencer stopped for {self.node_name}")

    def reset(self, preserve_seq: bool = True):
        with self._pending_lock, self._buffer_lock, self._history_lock, self._nack_lock:
            self._pending.clear()
            self._message_buffer.clear()
            self._message_history.clear()
            self._pending_nacks.clear()

            if preserve_seq:
                base = max(self._last_received_seq, self._sequence_num)
                self._sequence_num = base
                self._last_received_seq = base
            else:
                self._sequence_num = 0
                self._last_received_seq = 0

        logger.info(
            f"Node {self.node_name}: Sequencer reset (preserve_seq={preserve_seq}, "
            f"seq={self._sequence_num}, last={self._last_received_seq})"
        )

    def sync_sequence_num(self, last_seq: int):
        last_seq = int(last_seq)
        with self._pending_lock, self._buffer_lock, self._nack_lock:
            self._sequence_num = last_seq
            self._last_received_seq = last_seq
            self._message_buffer.clear()
            self._pending_nacks.clear()
        logger.info(f"Node {self.node_name}: Synced sequence number to {last_seq}")

    # ----------------------------
    # LEADER: broadcast + retries
    # ----------------------------
    def broadcast_phase_update(self, current_green, node_phases):
        peers = self.get_peers()
        if not peers:
            logger.warning(f"Node {self.node_name}: No peers to send to")
            return

        with self._pending_lock:
            self._sequence_num += 1
            seq = self._sequence_num

            payload = {"seq": seq, "current_green": current_green, "node_phases": node_phases}
            msg = {
                "type": message.TYPE_PHASE_UPDATE,
                "sender_id": self.node_id,
                "sender_name": self.node_name,
                "payload": payload,
            }

            self._pending[seq] = {
                "msg": msg,
                "missing": set(peers.keys()),
                "last_sent": time.time(),
                "retries": 0,
            }

        logger.info(f"Leader {self.node_name}: Broadcasting PHASE_UPDATE (seq={seq}, green={current_green})")

        # Store in history for NACK recovery
        self._store_in_history(seq, msg)

        for pid, pinfo in peers.items():
            try:
                self.send_message(pid, pinfo, msg)
            except Exception as e:
                logger.error(f"Failed to send phase update to {pid}: {e}")

    def handle_ack(self, msg):
        payload = msg.get("payload", {})
        seq = payload.get("seq")
        sender = msg.get("sender_id")
        if seq is None or sender is None:
            return

        seq = int(seq)
        with self._pending_lock:
            entry = self._pending.get(seq)
            if not entry:
                return

            entry["missing"].discard(sender)
            if not entry["missing"]:
                logger.info(f"Leader {self.node_name}: All ACKs received for seq={seq}")
                self._pending.pop(seq, None)

    # ----------------------------
    # OMISSION FAULT RECOVERY
    # ----------------------------
    def _store_in_history(self, seq, msg):
        """Store message in history for NACK-based recovery."""
        with self._history_lock:
            self._message_history[seq] = msg
            # Prune old entries if history too large
            if len(self._message_history) > self._max_history_size:
                oldest = min(self._message_history.keys())
                self._message_history.pop(oldest, None)

    def handle_nack(self, msg):
        """Leader handles NACK: resend requested sequence numbers."""
        payload = msg.get("payload", {})
        missing_seqs = payload.get("missing_seqs", [])
        sender_id = msg.get("sender_id")
        
        if not missing_seqs or sender_id is None:
            return
        
        peers = self.get_peers()
        sender_info = peers.get(sender_id)
        if not sender_info:
            logger.warning(f"Leader {self.node_name}: NACK from unknown peer {sender_id}")
            return
        
        logger.info(f"Leader {self.node_name}: Got NACK from {sender_id} for seqs {missing_seqs}")
        
        with self._history_lock:
            for seq in missing_seqs:
                seq = int(seq)
                original_msg = self._message_history.get(seq)
                if original_msg:
                    # Resend the original message
                    try:
                        self.send_message(sender_id, sender_info, original_msg)
                        logger.info(f"Leader {self.node_name}: Resent seq={seq} to {sender_id} (NACK recovery)")
                    except Exception as e:
                        logger.error(f"Leader {self.node_name}: Failed to resend seq={seq}: {e}")
                else:
                    logger.warning(f"Leader {self.node_name}: Seq={seq} not in history, can't recover")

    def _send_nack(self, missing_seqs, leader_id, leader_info):
        """Send NACK to leader requesting missing sequence numbers."""
        if not missing_seqs:
            return
        
        # Don't send NACK for seqs we've already requested
        with self._nack_lock:
            new_missing = [s for s in missing_seqs if s not in self._pending_nacks]
            if not new_missing:
                return
            for s in new_missing:
                self._pending_nacks.add(s)
        
        nack_msg = {
            "type": message.TYPE_NACK,
            "sender_id": self.node_id,
            "sender_name": self.node_name,
            "payload": {"missing_seqs": new_missing},
        }
        
        try:
            self.send_message(leader_id, leader_info, nack_msg)
            logger.info(f"Node {self.node_name}: Sent NACK for seqs {new_missing} to leader {leader_id}")
        except Exception as e:
            logger.error(f"Node {self.node_name}: Failed to send NACK: {e}")

    def _nack_retry_loop(self):
        """Periodically retry NACKs for missing sequences that haven't arrived."""
        nack_interval = getattr(config, "ACK_TIMEOUT", 0.5) * 2  # Check every 1 second
        
        while self._running:
            time.sleep(nack_interval)
            
            expected = self._last_received_seq + 1
            with self._buffer_lock:
                buffered_seqs = set(self._message_buffer.keys())
            
            if not buffered_seqs:
                continue
            
            # Find gaps: we have higher seqs buffered but missing some in between
            max_buffered = max(buffered_seqs)
            missing = []
            for seq in range(expected, max_buffered):
                if seq not in buffered_seqs:
                    missing.append(seq)
            
            if missing:
                # Get leader info to send NACK
                peers = self.get_peers()
                # Find the leader (sender of buffered messages)
                with self._buffer_lock:
                    sample_msg = next(iter(self._message_buffer.values()), None)
                if sample_msg:
                    leader_id = sample_msg.get("sender_id")
                    leader_info = peers.get(leader_id)
                    if leader_info:
                        self._send_nack(missing, leader_id, leader_info)

    def _resend_monitor_loop(self):
        ack_timeout = getattr(config, "ACK_TIMEOUT", 0.5)
        max_retries = getattr(config, "MAX_RETRIES", 3)

        while self._running:
            time.sleep(ack_timeout)

            now = time.time()
            with self._pending_lock:
                items = list(self._pending.items())

            for seq, entry in items:
                missing = entry["missing"]
                if not missing:
                    continue

                if now - entry["last_sent"] < ack_timeout:
                    continue

                if entry["retries"] >= max_retries:
                    logger.warning(
                        f"Leader {self.node_name}: Gave up on seq={seq}, missing ACKs from: {missing}"
                    )
                    with self._pending_lock:
                        self._pending.pop(seq, None)
                    continue

                entry["retries"] += 1
                entry["last_sent"] = now

                peers = self.get_peers()
                msg = entry["msg"]
                logger.info(f"Leader {self.node_name}: Retrying PHASE_UPDATE seq={seq} (retry #{entry['retries']})")

                for pid in list(missing):
                    pinfo = peers.get(pid)
                    if not pinfo:
                        continue
                    try:
                        self.send_message(pid, pinfo, msg)
                    except Exception as e:
                        logger.error(f"Retry send failed to {pid} for seq={seq}: {e}")

    # ----------------------------
    # FOLLOWER: ordered delivery
    # ----------------------------
    def handle_phase_update(self, msg, leader_id=None, leader_info=None):
        payload = msg.get("payload", {})
        seq = payload.get("seq")
        current_green = payload.get("current_green")
        node_phases = payload.get("node_phases")

        if seq is None:
            logger.warning(f"Node {self.node_name}: PHASE_UPDATE missing seq")
            return

        seq = int(seq)
        logger.info(f"Node {self.node_name}: Got PHASE_UPDATE (seq={seq}, green={current_green})")

        expected = self._last_received_seq + 1

        if seq < expected:
            logger.info(f"Node {self.node_name}: Duplicate/old seq={seq}, expected={expected}, ignoring")
            self._send_ack(msg, seq, leader_id, leader_info)
            return

        if seq > expected:
            logger.warning(f"Node {self.node_name}: Out of order! Expected {expected}, got {seq}")
            with self._buffer_lock:
                self._message_buffer[seq] = msg
            self._send_ack(msg, seq, leader_id, leader_info)
            
            # OMISSION FAULT RECOVERY: Send NACK for missing sequences
            missing_seqs = list(range(expected, seq))
            if missing_seqs and leader_id is not None and leader_info is not None:
                self._send_nack(missing_seqs, leader_id, leader_info)
            return

        self._process_in_order(msg)
        self._send_ack(msg, seq, leader_id, leader_info)

        while True:
            next_seq = self._last_received_seq + 1
            with self._buffer_lock:
                buffered = self._message_buffer.pop(next_seq, None)
            if not buffered:
                break
            self._process_in_order(buffered)
            self._send_ack(buffered, next_seq, leader_id, leader_info)

    def _process_in_order(self, msg):
        payload = msg.get("payload", {})
        seq = int(payload.get("seq", 0))
        current_green = payload.get("current_green")
        node_phases = payload.get("node_phases", {})

        logger.info(f"Node {self.node_name}: Processing seq={seq}, green={current_green}")
        self._last_received_seq = seq
        
        # Clear from pending NACKs since we received it
        with self._nack_lock:
            self._pending_nacks.discard(seq)

        try:
            self.on_phase_update(current_green, node_phases)
        except Exception as e:
            logger.error(f"Node {self.node_name}: Error applying phase update: {e}")

    def _send_ack(self, msg, seq, leader_id=None, leader_info=None):
        leader = leader_id if leader_id is not None else msg.get("sender_id")
        if leader is None:
            return

        peers = self.get_peers()
        info = leader_info if leader_info is not None else peers.get(leader)
        if not info:
            return

        ack = {
            "type": message.TYPE_ACK,
            "sender_id": self.node_id,
            "sender_name": self.node_name,
            "payload": {"seq": int(seq)},
        }
        try:
            self.send_message(leader, info, ack)
        except Exception as e:
            logger.error(f"Node {self.node_name}: Failed to send ACK for seq={seq} to leader {leader}: {e}")

    # ----------------------------
    # STATE SYNC for joining nodes
    # ----------------------------
    def get_state_for_sync(self):
        base = max(self._sequence_num, self._last_received_seq)
        return {"sequence_num": base}

    def apply_sync_state(self, state):
        seq = int(state.get("sequence_num", 0))
        with self._pending_lock, self._buffer_lock:
            self._sequence_num = seq
            self._last_received_seq = seq
            self._message_buffer.clear()
        logger.info(f"Node {self.node_name}: Applied sync state, seq={seq}")
