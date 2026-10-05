# System Design

This is a decentralized, high-throughput architecture for code execution.
The system is divided into four primary roles: the Gateway, the Orchestrator (Monitor and Assigner), the Worker, and the Agent.

## Gateway

The Gateway acts as the bridge between the high-level user interface and the low-level execution cluster.
It will be a personal computer running a high-level language environment (WebSockets) that communicates with the cluster using a custom C-library (TCP).
There is exactly one active Gateway in the network at any time, identified by a Virtual IP (VIP).
If the Gateway fails, a Monitor Orchestrator is promoted to take its place.
The new Gateway uses **Gratuitous ARP** to announce its MAC address for the existing VIP, though clients must reconnect to the new WebSocket instance as TCP state is not mirrored across machines.

## Orchestrator

The Orchestrator is the "Brain" of the system.
There must always be a minimum of two orchestrators to maintain a quorum and ensure redundancy.
The role is split into two specialized types to separate the Control Plane from the Data Plane.

### Monitor Orchestrator

The Monitor Orchestrator manages the cluster's health and configuration.
It receives heartbeats from all nodes (Gateways, Assigners, and Workers) and calculates global health using **Kernel PSI (Pressure Stall Information)**.
Instead of syncing full state, it uses **Action-Based Gossip (Event Sourcing)**, where nodes exchange timestamped logs of actions like promotions, task assignments, or node failures.
This allows every Monitor to merge logs and maintain a robust, consistent record of the system history even in unreliable networks.
The Monitor is responsible for elasticity; it can promote an "Empty Node" running the Agent process to a Worker or Assigner if the system load (PSI score) becomes too high.
It broadcasts its own heartbeats containing the "Health Scores" of all nodes to the rest of the cluster.

### Assigner Orchestrator

The Assigner Orchestrator handles the actual movement of code and data.
It receives code from the Gateway and dispatches it to a selected Worker based on the health data provided by the Monitor.
To ensure high availability, every Assigner has a **Buddy** (an Active-Active pair) that mirrors its pending and active task list in RAM.
If one Assigner in the pair crashes, the Buddy immediately takes over the active streams without losing the task.
The Assigner acts as a secure middleman, ensuring that Workers and Gateways never communicate directly, which prevents lateral movement attacks if a container is compromised.
It also manages **Backpressure** by monitoring buffer levels; if the Gateway cannot keep up with the output, the Assigner sends control signals to the Worker to pause execution.

## Worker

The Worker is the "Muscle" of the system, responsible for running untrusted code in a secure sandbox.
It maintains a three-thread model to handle communication, execution, and health monitoring independently.

### Communicator Thread

This thread maintains the connection with the Assigner Orchestrator.
On a new task, it spawns a Runner Thread and manages the lifecycle of the connection.

### Runner Thread

This thread handles the containerization logic.
It creates a child process using **Linux Namespaces** (PID, UTS, Network, and User) and **Cgroups v2** for resource limits.
The code is isolated via **pivot_root** into a minimal `RootFS` (like Alpine) and stripped of kernel capabilities to prevent host damage.
The Runner Thread implements **SIGSTOP/SIGCONT** flow control; it can pause the container instantly if the Assigner signals that the network buffers are full.
It also supports **Deterministic Replay**; if a container fails, the runner can restart it and compare the new output to the cached old output to determine if the execution should continue or alert the user of a non-deterministic crash.
In case the cached output was too huge, the thread just exits and the client has to retry.
It manages the stream of input and output to and from the container.

### Heartbeat Thread

This thread monitors the local machine's vitals, specifically the **PSI (Pressure Stall Information)**.
It communicates with the other Worker threads via **Unix Domain Sockets (UDS)** to ensure they haven't deadlocked.
If the Communicator thread fails to respond to 5 consecutive internal pings, the Heartbeat Thread reports the machine as "Dead" to the Monitor Orchestrator.

## Agent

The Agent is a lightweight daemon that runs on every "Empty Machine" in the lab.
Its only job is to broadcast a periodic UDP heartbeat saying "I am alive and available."
It waits for a promotion command from a Monitor Orchestrator to become either an Orchestrator or a Worker.

## Specifications

1. **Consensus Algorithm:** RAFT is used for leader election among Monitors and for critical role promotions.
2. **State Sync:** Action-Based Gossip (Event Sourcing) with high-resolution timestamps for merging logs.
3. **Data Flow:** A 4-Hop model (Client ➔ Gateway ➔ Monitor/Assigner ➔ Worker) to separate heavy I/O from data throughput.
4. **Protocols:** TCP is used for code payloads and I/O streaming; UDP is used for heartbeats, discovery, and Raft elections.
5. **Flow Control:** Backpressure is managed via `SIGSTOP` (pause) and `SIGCONT` (resume) signals sent to the container process.
6. **Health Monitoring:** Kernel PSI (Pressure Stall Information) is used instead of raw container counts to determine node "Health Scores".
7. **Discovery:** Nodes join via UDP broadcast followed by a 3-way TCP handshake with a limited `listen()` queue to prevent thundering herd issues.

## Failure Scenarios and Solutions

### 1. The last worker crashes

The Monitor Orchestrator notices the missing heartbeats and checks for available Assigner pairs to be demoted into Worker nodes. If no such pairs are available (they are less than or equal to the minimum), it promotes an available agent to a Worker node to ensure the system remains functional.

### 2. The last monitoring orchestrator crashes

Assigner Orchestrators detect the loss of heartbeats. They enter Phase 1 of election (acknowledgment of failure). Once a majority is reached, they move to Phase 2 (Random Number/Log Sequence comparison) and Phase 3 (Voting). The winner becomes the new Monitor and promotes its Buddy to ensure the Monitor role has redundancy.

### 3. An assigning orchestrator crashes

Its Buddy instantly takes over the active tasks using the mirrored task list in RAM. The Monitor Orchestrator eventually promotes an Agent or a Worker to become the new Buddy to restore the pair's redundancy.

### 4. A buddy pair (Assigners) crashes simultaneously

This is the only scenario where active task state is lost. The Monitor Orchestrator notifies the Gateway to reset those specific client connections. It then promotes new nodes to replace the failed pair and resumes service.

### 5. The Gateway crashes

The Monitor Orchestrator promotes itself (or another Monitor) to the Gateway role. It uses Gratuitous ARP to claim the VIP. Because WebSocket/TCP state cannot be perfectly repaired across different machines without massive overhead, the clients are forced to reconnect to the new Gateway instance.

### 6. Resource limit (All machines occupied)

The Monitor Orchestrator sees the high PSI scores across the cluster. It will begin queuing requests at the Gateway. If the "Empty Machine" pool is exhausted, the Monitor will not promote any more nodes and will send a "System Busy" signal to the Gateway.

### 7. A container fails mid-execution

The Runner Thread attempts a transparent re-run. If the second run produces the exact same output as the first (deterministic), it continues streaming where it left off. If the output differs, the Runner pauses the container (`SIGSTOP`), informs the client of the discrepancy, and waits for a "Resume" or "Restart" command.
