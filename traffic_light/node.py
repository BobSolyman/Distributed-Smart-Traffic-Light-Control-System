import socket
import threading
import time
import logging

from . import config
from . import message
from . import discovery
from . import heartbeat
from . import election
from . import sequencer

logger = logging.getLogger(__name__)


class TrafficLightNode:
    def __init__(self, node_name, node_id):
        """
        Initialize the traffic light node and its components.
        """
        self.node_name = node_name
        self.node_id = node_id

        # Determine unicast port for this node (unique per node if multiple on one host)
        self.unicast_port = config.DEFAULT_TCP_PORT + int(node_id)

        # Bind UDP socket for unicast messages
        self._unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Allow immediate reuse of the port if possible
        try:
            self._unicast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception:
            pass
        try:
            if hasattr(socket, "SO_REUSEPORT"):
                self._unicast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass

        # Bind to all interfaces on the designated unicast port
        self._unicast_sock.bind(("", self.unicast_port))
        logger.info(f"Node {self.node_name} binding unicast UDP socket to port {self.unicast_port}")

        # Windows-specific: prevent recvfrom() from throwing ConnectionResetError on ICMP "Port Unreachable"
        try:
            if hasattr(socket, "SIO_UDP_CONNRESET"):
                self._unicast_sock.ioctl(socket.SIO_UDP_CONNRESET, 0)
        except Exception:
            pass

        # Initialize services
        self.heartbeat = heartbeat.HeartbeatMonitor(self.node_id)

        # Discovery will announce and listen for peers. Update heartbeat on discovery events.
        self.discovery = discovery.Discovery(
            self.node_id,
            self.unicast_port,
            on_peer_discovered=self.heartbeat.update_peer
        )

        # Election service for leader election
        self.election = election.ElectionService(
            self.node_id, self.node_name,
            get_peers=self.heartbeat.get_peers,
            on_leader_elected=self._handle_leader_elected,
            send_message=self._send_unicast
        )

        # Sequencer for reliable ordered multicast
        self.sequencer = sequencer.Sequencer(
            self.node_id, self.node_name,
            get_peers=self.heartbeat.get_peers,
            send_message=self._send_unicast,
            on_phase_update=self._handle_phase_update
        )

        # Traffic light current phase for this node (start at RED)
        self.current_phase = "RED"

        # Internal state flags
        self._running = False
        self._phase_thread = None

        # Heartbeat callbacks
        self.heartbeat.on_node_joined = self._handle_node_joined
        self.heartbeat.on_node_left = self.election.on_peer_failed

    def start(self):
        """
        Start all components of the node (discovery, heartbeat, communication).
        Begin leader election process and traffic light coordination.
        """
        if self._running:
            return
        self._running = True

        self.discovery.start()
        self.heartbeat.start()
        self.sequencer.start()

        recv_thread = threading.Thread(target=self._unicast_receive_loop, daemon=True)
        recv_thread.start()

        time.sleep(1)
        self.election.start_election()
        logger.info(f"Node {self.node_name} (ID {self.node_id}) started")

    def stop(self):
        """
        Stop the node gracefully, shutting down all components and threads.
        """
        self._running = False

        try:
            self.discovery.stop()
        except Exception as e:
            logger.error(f"Error stopping discovery: {e}")

        try:
            self.heartbeat.stop()
        except Exception as e:
            logger.error(f"Error stopping heartbeat: {e}")

        try:
            self.sequencer.stop()
        except Exception as e:
            logger.error(f"Error stopping sequencer: {e}")

        if self._phase_thread and self._phase_thread.is_alive():
            logger.info("Waiting for phase loop to terminate...")

        try:
            self._unicast_sock.close()
        except Exception as e:
            logger.error(f"Error closing unicast socket: {e}")

        logger.info(f"Node {self.node_name} stopped")

    def _send_unicast(self, peer_id, peer_info, msg):
        """
        Send a message (dict) to the given peer via unicast UDP.
        """
        try:
            data = message.serialize(msg)
            self._unicast_sock.sendto(data, (peer_info["ip"], peer_info["port"]))
        except Exception as e:
            logger.error(f"Failed to send message to {peer_id} at {peer_info}: {e}")
            raise

    def _unicast_receive_loop(self):
        """
        Background thread for receiving unicast messages and dispatching to services.
        """
        logger.info(f"Node {self.node_name}: Listening for unicast messages on port {self.unicast_port}")

        while self._running:
            try:
                data, addr = self._unicast_sock.recvfrom(config.BUFFER_SIZE)
            except ConnectionResetError:
                # Windows: ignore ICMP reset noise and keep receiving
                continue
            except OSError:
                break

            if not data:
                continue

            msg = message.deserialize(data)
            if not msg:
                continue

            msg_type = msg.get("type")
            sender_id = msg.get("sender_id")

            # Update heartbeat for this peer (refresh last_seen)
            if sender_id is not None and sender_id != self.node_id:
                self.heartbeat.update_peer(sender_id, {"ip": addr[0], "port": addr[1]})

            # Dispatch based on message type
            if msg_type == message.TYPE_ELECTION:
                peer_info = self.heartbeat.get_peers().get(sender_id, None)
                if not peer_info:
                    peer_info = {"ip": addr[0], "port": addr[1]}
                msg["sender_info"] = peer_info
                self.election.handle_election_message(msg)

            elif msg_type == "ANSWER":
                self.election.handle_answer_message(msg)

            elif msg_type == message.TYPE_COORDINATOR:
                # Add sender info for ACK response
                peer_info = self.heartbeat.get_peers().get(sender_id, None)
                if not peer_info:
                    peer_info = {"ip": addr[0], "port": addr[1]}
                msg["sender_info"] = peer_info
                self.election.handle_coordinator_message(msg)

            elif msg_type == message.TYPE_COORDINATOR_ACK:
                # Leader receives ACK for COORDINATOR message
                self.election.handle_coordinator_ack(msg)

            elif msg_type == message.TYPE_PHASE_UPDATE:
                leader_info = self.heartbeat.get_peers().get(sender_id, None)
                self.sequencer.handle_phase_update(msg, leader_id=sender_id, leader_info=leader_info)

            elif msg_type == message.TYPE_ACK:
                self.sequencer.handle_ack(msg)

            elif msg_type == message.TYPE_NACK:
                # Leader handles NACK: resend missing sequences
                self.sequencer.handle_nack(msg)

            elif msg_type == "STATE_SYNC":
                state = msg.get("payload", {})
                self.sequencer.apply_sync_state(state)

            elif msg_type == message.TYPE_HEARTBEAT:
                pass

            else:
                logger.warning(f"Node {self.node_name}: Unhandled message type {msg_type} from {sender_id}")

        logger.info(f"Node {self.node_name}: Unicast receive loop terminated")

    def _handle_leader_elected(self, leader_id, leader_name):
        """
        Callback invoked when a leader is elected (either this node or another).
        """
        if leader_id == self.node_id:
            logger.info(f"Node {self.node_name}: Became leader")

            # Use sync state baseline (avoids private attr dependency)
            try:
                last_seq = int(self.sequencer.get_state_for_sync().get("sequence_num", 0))
            except Exception:
                last_seq = 0

            self.sequencer.sync_sequence_num(last_seq)

            if self._phase_thread is None or not self._phase_thread.is_alive():
                self._phase_thread = threading.Thread(target=self._phase_update_loop, daemon=True)
                self._phase_thread.start()

        else:
            logger.info(f"Node {self.node_name}: Leader is now {leader_name} (ID {leader_id})")

            # Critical: do NOT reset seq to 0, or you will get "Expected 1, got 51"
            self.sequencer.reset(preserve_seq=True)

    def _handle_node_joined(self, node_id, info):
        """
        Callback when a new node is discovered.
        If this node is leader, send state synchronization to the new node.
        """
        logger.info(f"Node {self.node_name}: Node {node_id} joined with info {info}")

        if self.election.is_leader and node_id != self.node_id:
            state = self.sequencer.get_state_for_sync()
            sync_msg = {
                "type": "STATE_SYNC",
                "sender_id": self.node_id,
                "sender_name": self.node_name,
                "payload": state,
            }
            try:
                self._send_unicast(node_id, info, sync_msg)
                logger.info(f"Node {self.node_name}: Sent state sync to node {node_id}")
            except Exception as e:
                logger.error(f"Failed to send state sync to new node {node_id}: {e}")

    def _handle_phase_update(self, current_green, node_phases):
        """
        Callback invoked when a phase update is applied (for followers).
        Update this node's current phase based on the new state.
        """
        phase = node_phases.get(str(self.node_id)) or node_phases.get(self.node_id)
        if phase is None:
            phase = "RED"
        self.current_phase = phase
        logger.info(f"Node {self.node_name}: Phase updated to {self.current_phase}")

    def _phase_update_loop(self):
        """
        Leader-only loop to send out traffic phase updates in a round-robin fashion.
        Runs as long as this node remains leader.
        """
        logger.info(f"Node {self.node_name}: Phase update loop started (leader)")

        while self._running and self.election.is_leader:
            peers = self.heartbeat.get_peers()
            all_nodes = sorted(list(peers.keys()) + [self.node_id])
            if not all_nodes:
                all_nodes = [self.node_id]

            for node_id in all_nodes:
                if not self._running or not self.election.is_leader:
                    break

                node_phases = {str(nid): "RED" for nid in all_nodes}
                current_green_id = node_id
                node_phases[str(current_green_id)] = "GREEN"

                self.current_phase = "GREEN" if current_green_id == self.node_id else "RED"
                logger.info(f"Leader {self.node_name}: Turning node {current_green_id} GREEN")
                self.sequencer.broadcast_phase_update(current_green_id, node_phases)
                time.sleep(config.DEFAULT_GREEN_DURATION)

                if not self._running or not self.election.is_leader:
                    break

                node_phases[str(current_green_id)] = "YELLOW"
                self.current_phase = "YELLOW" if current_green_id == self.node_id else "RED"
                logger.info(f"Leader {self.node_name}: Turning node {current_green_id} YELLOW")
                self.sequencer.broadcast_phase_update(current_green_id, node_phases)
                time.sleep(config.DEFAULT_YELLOW_DURATION)

        logger.info(f"Node {self.node_name}: Phase update loop ended")
