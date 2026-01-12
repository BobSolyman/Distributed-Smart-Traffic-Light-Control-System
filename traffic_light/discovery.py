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
    def __init__(self, node_id, unicast_port, on_peer_discovered=None):
        self.node_id = node_id
        self.unicast_port = unicast_port
        self.on_peer_discovered = on_peer_discovered
        self._running = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass

        self.sock.bind(('', config.MULTICAST_PORT))

        group = socket.inet_aton(config.MULTICAST_GROUP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    def start(self):
        self._running = True
        threading.Thread(target=self._listen_loop, daemon=True).start()
        threading.Thread(target=self._announce_loop, daemon=True).start()
        logger.info(f"Discovery started for node {self.node_id}")

    def stop(self):
        self._running = False
        try:
            self.sock.close()
        except Exception:
            pass
        logger.info("Discovery stopped")

    def _announce_loop(self):
        while self._running:
            try:
                msg = message.create_message(
                    message.TYPE_DISCOVERY,
                    self.node_id,
                    {'port': self.unicast_port}
                )
                data = message.serialize(msg)

                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
                s.sendto(data, (config.MULTICAST_GROUP, config.MULTICAST_PORT))
                s.close()
            except Exception as e:
                logger.error(f"Error sending announcement: {e}")

            time.sleep(config.DISCOVERY_INTERVAL)

    def _listen_loop(self):
        while self._running:
            try:
                data, addr = self.sock.recvfrom(config.BUFFER_SIZE)
                msg = message.deserialize(data)

                if msg and msg['type'] == message.TYPE_DISCOVERY:
                    sender = msg['sender_id']
                    if sender != self.node_id:
                        payload = msg.get('payload', {})
                        peer_info = {
                            'ip': addr[0],
                            'port': payload.get('port', config.DEFAULT_TCP_PORT),
                            'last_seen': time.time()
                        }
                        if self.on_peer_discovered:
                            self.on_peer_discovered(sender, peer_info)

            except OSError:
                break
            except Exception as e:
                logger.error(f"Error in discovery listener: {e}")
