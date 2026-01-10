"""
config.py - Configuration constants for the Distributed Traffic Light System.
"""

# Networking Configuration
MULTICAST_GROUP = '239.255.255.250'  # As per System Architecture Diagram
MULTICAST_PORT = 5007
DEFAULT_TCP_PORT = 6000
BUFFER_SIZE = 4096

# Timing Configuration (in seconds)
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.0  # As per Fault Tolerance section
DISCOVERY_INTERVAL = 2.0
ELECTION_TIMEOUT = 5.0

# Node Configuration
# Timeout for waiting for an ACK in reliable multicast
ACK_TIMEOUT = 0.5  # As per Reliable Ordered Multicast section
MAX_RETRIES = 3

# Simulation
DEFAULT_GREEN_DURATION = 5.0
DEFAULT_YELLOW_DURATION = 2.0
