# Distributed Smart Traffic Light Control System

A decentralized, fault-tolerant traffic light control system designed for distributed environments. This system coordinates traffic phases across multiple intersections without a single point of failure (SPOF), utilizing dynamic leader election and reliable multicast for state synchronization.

## System Architecture

The system is built on a peer-to-peer architecture where nodes (traffic lights) effectively discover each other, elect a coordinator, and maintain a consistent global state using a primary-backup replication model.

### Communication Middleware (`/traffic_light`)

The core networking layer was implemented to handle group membership and failure detection autonomously, decoupling the application logic from the underlying network topology.

#### 1. Dynamic Discovery Service (`discovery.py`)
*   **Mechanism:** UDP Multicast (Port 5007)
*   **Protocol:** `239.255.255.250` (SSDP Standard Group)
*   **Methodology:** Implements a "Zero-Configuration" networking model. Nodes run a background **Announcer Thread** that periodically broadcasts identity/capability packets and a **Listener Thread** that maintains a real-time "View" of the active group. This allows the cluster to scale elastically as nodes join or leave.

#### 2. Failure Detection (`heartbeat.py`)
*   **Algorithm:** $\phi$-style Timeout Detector (3.0s threshold)
*   **Methodology:** Provides a liveness monitor service. It aggregates heartbeat signals from the Discovery layer to maintain an accurate health status of all peers. It utilizes a soft-state approach where peers are implicitly marked as "failed" if no signals are received within the 3-second window, acting as the primary trigger for the Leader Election process.

#### 3. Protocol & Serialization (`message.py`)
*   **Format:** JSON-based Application Layer Protocol
*   **Methodology:** Uses structured JSON marshalling for language-independent data representation. Every message encapsulates a standard header (Type, SenderID, Timestamp) and a variable payload. This ensures interoperability and simplifies debugging of distributed traces.

#### 4. System Configuration (`config.py`)
*   **Methodology:** Centralized Configuration Management
*   **Design:** Defines strict contract parameters for timeouts ($T_{elect}$=5s, $T_{hb}$=3s) and network addresses. This single source of truth prevents split-brain scenarios caused by configuration drift between nodes.

## Configuration

The system functions on the following standard ports:
- **Multicast Discovery**: `239.255.255.250:5007`
- **TCP Communication**: `Port 6000` (Default)