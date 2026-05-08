# System Design
This is a decentralized architecture with 3 roles a Gateway, Orchestrators and Workers


## Gateway
There will be exactly one node in the network that will be a Gateway. This will be a personal computer and it will have a specific ip address inside the network.
If a Gateway fails, then an Orchestrator will become a Gateway and using "Gratuitous ARP" and it will continue the tcp connection using "Linux TCP Connection Repair".


## Orchestrator
The minimum number of orchestrators in the network will be 2, at any point in time the network should have more than or equal to 2 orchestrators.
There are 2 types of Orchestrators, assigner Orchestrator and monitor Orchestrator

### Assigner Orchestrator
This type of an orchestrator will get code from the monitor orchestrator and will assign a task to a worker.
According to the heartbeat information from the monitor orchestrator, this orchestrator will assign tasks to the workers.
In case a worker fails due to some reason, this orchestrator will re-assign the same tasks to some other worker.
If the monitor orchestrator fails to send a heartbeat 5 consecutive times, election will take place and one of the orchestrator becomes the new monitor orchestrator.
The election will have 3 phases, first acknowledgement that the election is valid and no one has received heartbeats, the second is which node has which random number, and the third is that everyone thinks that the new node is elected.
This orchestrator will have a buddy to which it will send its ongoing and upcoming tasks so that if this node fails the buddy can continue. Similarly the buddy will also send its tasks and this node will store it with itself.
The two nodes will send each other heartbeat signals to let each other know they are living, and similarly on not receiving 5 consecutive heartbeats, the other node takes over.
This orchestrator will also listen to the workers and streams any output from the worker to the gateway and streams input from the gateway to the worker.

### Monitor Orchestrator
This type of an orchestrator will receive heartbeats from all types of nodes, the Gateway, the Orchestrators and the Workers. The Orchestrators and Workers send a heartbeat with the machine's PSI. This informs this orchestrator on where to assign new tasks.
This type of an orchestrator converts an empty machine to an Orchestrator or Worker depending on the requirement, if the current system load is too high then this orchestrator will convert an empty node into either another orchestrator or another worker and if the load is less then some nodes are not sent tasks effectively putting them to halt.
This orchestrator will also broadcast its own heartbeats which consist of worker and orchestrator scores.


## Worker
The number of workers at any time should be more than or equal to 1
A worker will be a process on a machine with 3 threads.

### Communicator Thread
This thread communicates with the orchestrator thread and creates a new Runner Thread on new connections. It also listens for finished communications and kills the started threads.

### Runner Thread
This thread creates a new child process which runs the given code in a container. It esssentially manages the container by streaming output to the orchestrator and streaming input into the container.

### Heartbeat Thread
This thread communicates with the communicator thread using UDS (Unix Domain Sockets) and keeps a ping pong communication going on. And on no reponse for 5 consecutive pings, this will send a dead heartbeat to the orchestrator.


## Specifications
1. Selection Algorithm: RAFT
2. 3 way handshake to send a heartbeat for the first time, this is required so that the heartbeat is sent to nearest orchestrator of type 2 in the network.
3. TCP is only used to transfer code among nodes and for streaming input and output. For everything else like heartbeats and RAFT UDP is used.
4. All the machines will never be used, the system is designed to minimize failure points. Hence the system will use the minimum number of nodes required according to the traffic. It will first load currently used machines and if the machines are occupied, it will start processing on new machines.
5. Worker heartbeats consist of the worker machine's PSI
6. Assigning orchestrator heartbeats consist of machine's PSI, current tasks at hand and it buddy's IP address.



## Failure Scenarios and their Solutions:

### Failure Scenarios:
1. The last worker crashes
2. The last monitoring orchestrator crashes
3. An assigning orchestrator crashes
4. The last assigning orchestrator crashes
5. The gateway crashes
6. All machines are occupied
7. A container fails mid-execution



### Solutions (Tentative):
1. The monitor orchestrator now no longer hears the worker for 5 consecutive heartbeats. And since it sends heartbeats as a broadcast, the assigning orchestrators would know that their worker is not responding. Hence they will try to switch to a new worker. Seeing that no new worker is available the assigning orchestrators would elect one from themselves and demote that assigning orchestrator and its buddy to worker nodes.
2. The assigning orchestrators won't get monitor orchestrator's heartbeats for 5 consecutive times and there will be an election to choose a new monitoring orchestrator. Now there will be 3 phases to the election.
Phase 1 is the nodes asking each other whether everyone cannot hear the monitoring orchestrator. On getting 50% + 1 votes then the nodes proceed to Phase 2.
Phase 2 involves nodes sending out their random numbers to the network and receiving random numbers from other nodes.
Phase 3 involves voting for the node with the highest random number. Here too a 50% + 1 majority in votes is required to proceed.
Finally the selected assigning orchestrator and its buddy are made the new monitoring orchestrators.
3. If an assigning orchestrator crashes its buddy will continue its work and the monitoring orchestrator would eventually after 5 missing heartbeats notice that an assigning orchestrator is dead. So it will either hold an election among monitoring orchestrators and select a new assigning orchestrator and assign the same buddy to it. Or in case the monitoring orchestrators are few in number, a worker will be selected and made the new assigning orchestrator.
4. This is a highly unlikely scenario because both the nodes in a buddy pair are unlikely to crash together. But in case this happens, the client's work is lost and the monitoring orchestrator sees two assigning orchestrators are down. Then it checks for the buddy's of both nodes in the network, since in this case both were a part of the same pair the monitoring orchestrator won't find the buddy of either machine alive. Now the monitoring orchestrator communicates with the gateway and resets its connection by assigning new assigning orchestrators to the gateway.
5. The monitoring orchestrator also listens for heartbeats from the gateway and in case 2 consecutive heartbeats are missed from the gateway the monitoring orchestrator promotes one of the other monitoring orchestrator to the gateway using the RAFT elections. The new gateway then uses the Gatutious ARP to tell the network it is the new gateway. It also uses Linux TCP connection repair to continue the communications with all the attached clients without them noticing.
6. The monitoring orchestrator will get a new request from the gateway but there won't be any orchestrator that is below capacity hence a new empty machine would be promoted to an assigning orchestrator. In case there is an assinging orchestrator but not a worker, the assigning orchestrator would promote an empty node to a worker node.
7. If the user had previously interacted with the container with some input data streamed in, the runner thread would exit with an error code and the communicator thread would tell the assigning orchestrator about the failure. This would then be communicated to the client via the gateway. In case only output was streamed, the runner thread notes down the last output character number streamed and it would recreate the container, once the container is running again and reaches the current output character the output streaming starts again.



## Unsolved Questions:
