"""
discovery.py - UDP Multicast Discovery for Traffic Light Nodes.

Nodes use this module to announce their presence and discover other nodes.
"""

import socket
import struct
import threading
import time
import logging
from . import config
from . import message

logger = logging.getLogger(__name__)

class Discovery:
    def __init__(self, node_id, on_peer_discovered=None):
        """
        Initialize Discovery service.
        
        Args:
            node_id (str): Unique ID of this node.
            on_peer_discovered (callable): Callback when a peer updates (optional).
        """
        self.node_id = node_id
        self.on_peer_discovered = on_peer_discovered
        self._running = False
        
        # Socket setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        
        # Allow multiple sockets to use the same port
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # On some systems (Mac), SO_REUSEPORT might be needed too
            if hasattr(socket, 'SO_REUSEPORT'):
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
             pass

        self.sock.bind(('', config.MULTICAST_PORT))
        
        # Join Multicast Group
        group = socket.inet_aton(config.MULTICAST_GROUP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        # Set TTL for multicast (1 for local network)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    def start(self):
        """Start discovery threads."""
        self._running = True
        
        # Listener Thread
        self.listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
        self.listener_thread.start()
        
        # Announcer Thread
        self.announcer_thread = threading.Thread(target=self._announce_loop, daemon=True)
        self.announcer_thread.start()
        
        logger.info(f"Discovery started for node {self.node_id}")

    def stop(self):
        """Stop discovery threads."""
        self._running = False
        # Create a dummy socket to unblock recv if needed, or just let daemon threads die (if main exits)
        # But properly closing:
        try:
            self.sock.close()
        except:
            pass
        logger.info("Discovery stopped")

    def _announce_loop(self):
        """Periodically broadcast presence."""
        while self._running:
            try:
                # Payload can include TCP port for direct connection
                msg = message.create_message(
                    message.TYPE_DISCOVERY, 
                    self.node_id, 
                    {'port': config.DEFAULT_TCP_PORT}
                )
                data = message.serialize(msg)
                
                # Send to multicast group
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
                sock.sendto(data, (config.MULTICAST_GROUP, config.MULTICAST_PORT))
                sock.close()
                
            except Exception as e:
                logger.error(f"Error sending announcement: {e}")
            
            time.sleep(config.DISCOVERY_INTERVAL)

    def _listen_loop(self):
        """Listen for multicast messages."""
        while self._running:
            try:
                data, addr = self.sock.recvfrom(config.BUFFER_SIZE)
                msg = message.deserialize(data)
                
                if msg and msg['type'] == message.TYPE_DISCOVERY:
                    sender = msg['sender_id']
                    if sender != self.node_id:
                        # Extract info
                        payload = msg.get('payload', {})
                        peer_info = {
                            'ip': addr[0],
                            'port': payload.get('port', config.DEFAULT_TCP_PORT),
                            'last_seen': time.time()
                        }
                        
                        if self.on_peer_discovered:
                            self.on_peer_discovered(sender, peer_info)
                            
            except OSError:
                # Socket closed
                break
            except Exception as e:
                logger.error(f"Error in discovery listener: {e}")
