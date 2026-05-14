# Node Life Cycle

## Ports
- Empty nodes listen and send on port 8000, broadcast
- Empty nodes vote on port 8001, broadcast
- Gateway node sends requests from port number starting from 9000
- Gateway sends heartbeats from port 8000, broadcast
- Monitor nodes listen node heartbeats at 8000, broadcast
- Monitor nodes send heartbeats at 8002, broadcast
- Monitor nodes listen and send to gateway messages at 8003, unicast
- Monitor nodes sync information every 5 heartbeats at port 8004, broadcast
- Assigner nodes send heartbeats from port 8000, broadcast
- Assigner nodes listen for monitor heartbeats on port 8002, broadcast
- Assigner streams input and output of code via the same port as the Gateway port, unicast
- Assigner nodes communicate to the worker nodes via port 8003, unicast
- Worker nodes send heartbeats from port 8000, broadcast
- Worker nodes listen to assigner commands at port 8003, unicast
- Worker nodes' runner threads sends and receives the output and input on the same port as the Gateway port, unicast


## Empty Node
- When a node starts it first sends out a broadcast signal waiting for some monitor to reply back
- If it does not detect a monitor for 20 consecutive heartbeats, it starts an election with all the nodes it can hear.
- Using the RAFT protocol a new monitor is selected and that monitor then builds the system.


## Monitor Node
- When this node becomes a monitor it first tries to connect with other monitors and waits for 10 consecutive heartbeats for their responses.
- If no monitor responds, this monitor will then assume the system is starting from scratch and will listen to any incoming request from any node. It notes down the ip of the sender and the role listed in the packet. It then gets the minimum constraints from the gateway.
- It first creates the minimum number of assigners, then the minimum number of workers and then the minimum number of monitors (excluding itself).
- If monitors do respond, this monitor assumes it was promoted and would request information from other nodes. And then keep its lifecycle running.
- It keeps listening other nodes' heartbeats and sends its own heartbeats. Its own heartbeat consists of all nodes in the network with their IP and the PSIs of the worker nodes.
- It listens for requests from the gateway and replies with the least occupied assigner's IP


## Assigner Node
- When this node becomes an assigner, it assumes a monitor promoted it. so it waits for the first heartbeat of a monitor and does a 3-way handshake with it. Then it starts sending its heartbeats to the monitor.
- When the Gateway approaches it the assigner selects a worker node based on the last monitor heartbeat and contacts it. Both of them then switch the ports of communication and start streaming inputs and outputs.


## Worker Node
- When this node becomes a worker, it assumes a monitor has promoted it. so it waits for the first heartbeat of a monitor and does a 3-way handshake with it. Then it starts sending its heartbeat to the monitor.
- When an assigner assigns a task to it, it first checks it availabilty and then creates a new runner thread.
- The runner thread then switches to the preferred port and creates a new container and starts running the code in it.


## Node Numbers
- Empty Node - 0
- Gateway - 1
- Monitor - 2
- Assigner - 3
- Worker - 4
