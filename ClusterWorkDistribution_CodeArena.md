# CodeArena Workload Split

| Role | Profile | Key Ownership |
| --- | --- | --- |
| **Person 1** | **Hardware/Network Nerd** | The Network Fabric (UDP Discovery, Raft, ARP Failover, Heartbeats). |
| **Person 2** | **OS/Distributed Nerd** | The Sandbox (Namespaces, Cgroups, pivot_root, SIGSTOP Control). |
| **Person 3** | **Software Architect** | The Bridge (WebSocket Gateway, High-level API Integration, Result Streaming). |

---

# Finalized Timeline (6 Weeks)

## Week 1: The Network Fabric & Skeleton

* **Goal:** All nodes can "see" each other and the Gateway can talk to a Worker.
* **Person 1:** Build the **UDP Discovery Agent** and the **Handshake** logic. Implement the basic UDP heartbeat packet structure.
* **Person 2:** Set up the **C Socket Boilerplate** for all nodes. Create the **UDS (Unix Domain Socket)** communication for the internal Worker threads.
* **Person 3:** Design the **Packet Protocol** (The headers for Task ID, Payload Size, etc.). Start the **Raft Leader Election** logic for Monitors.

## Week 2: The Sandbox (The Core)

* **Goal:** A "Hello World" runs in a cage.
* **Person 1:** Implement the **Gossip Protocol** (Action Log syncing) between Monitors.
* **Person 2:** The **Container Sprint**. Implement `unshare`, `cgroups v2` limits, and `pivot_root`. Ensure the Worker can spawn a process with zero host visibility.
* **Person 3:** Build the **Buddy System** logic. Ensure Assigners are mirroring task states in RAM.

## Week 3: The API Bridge & Streaming

* **Goal:** High-level language talks to the C-cluster.
* **Person 1:** Implement the **Gratuitous ARP** logic for the Gateway VIP failover.
* **Person 2:** Implement the **SIGSTOP/SIGCONT** backpressure logic within the Runner thread.
* **Person 3:** The **Bridge Sprint**. Write the wrapper (FFI or Socket-based) that allows your high-level Gateway (Node/Python/Go) to send code to the C-library.

## Week 4: Platform Integration (Phase A)

* **Goal:** CodeArena "Backend" meets the "Frontend" platform.
* **Team Task:** Review the existing platform code together.
* **Focus:** Mapping the platform's database (Student IDs, Problem IDs) to the cluster's **Task IDs**.
* **Testing:** Run "Dummy" codes that simulate long-running tasks and infinite loops to see if the Cgroups catch them.

## Week 5: Platform Integration (Phase B) & Hardening

* **Goal:** Stability and Deterministic Replays.
* **Person 1:** Stress test the Monitor's Gossip protocol under heavy load.
* **Person 2:** Fine-tune the **Transparent Re-run** logic for failed containers.
* **Person 3:** Finalize the **Result Streaming** UI (handling the 1500-char chunks on the frontend).

## Week 6: Chaos Week & Security

* **Goal:** Break the system.
* **Task:** Physically unplug lab PCs mid-execution.
* **Task:** Attempt "Container Escapes" (e.g., trying to write to `/etc/` from the student code).
* **Task:** Monitor the lab PC performance while someone plays *Valorant* on the side.

---

### A Note for the Team

The most dangerous part of this timeline is **Week 3 Integration**. If the C-library and the High-level script don't agree on the packet format, the whole system will segfault. Person 3 needs to define the "Contract" (the `struct` format) on Day 1.
