# Node Life Cycle

## Ports
- 8000 - 8499 is for broadcast
- 8500 - 8999 is for unicast

- Empty nodes ask for monitors on port 8000, broadcast
- Empty nodes vote on port 8001, broadcast
- Empty nodes listen monitor hearbeats on port 8002, broadcast
- Empty nodes send heartbeats to monitors on port 8500, unicast

- Gateway node sends requests from port number starting from 9000
- Gateway sends heartbeats from port 8000, broadcast

- Monitor nodes answer node questions at 8000, broadcast
- Monitor nodes send heartbeats at 8002, broadcast
- Monitor nodes listen and send to gateway messages at 8501, unicast
- Monitor nodes sync information every 5 heartbeats at port 8003, broadcast
- Monitor nodes listen to node heartbeats on port 8500, unicast
- Monitor nodes vote for spawing new nodes on port 8004, broadcast

- Assigner nodes ask for monitors on port 8000, broadcast
- Assigner nodes send heartbeats from port 8500, unicast
- Assigner nodes listen for monitor heartbeats on port 8002, broadcast
- Assigner streams input and output of code via the same port as the Gateway port, unicast
- Assigner nodes communicate to the worker nodes via port 8502, unicast

- Worker nodes ask for monitors on port 8000, broadcast
- Worker nodes send heartbeats from port 8500, unicast
- Worker nodes listen monitor heartbeats at port 8002, broadcast
- Worker nodes listen to assigner commands at port 8502, unicast
- Worker nodes' runner threads sends and receives the output and input on the same port as the Gateway port, unicast


## Empty Node
- When a node starts it first sends out a broadcast signal waiting for some monitor to reply back
- If it does not detect a monitor for 5 consecutive heartbeats, it starts an election with all the nodes it can hear.
- Using the RAFT protocol a new monitor is selected and that monitor then builds the system.
- if a monitor replies, it does a 3 way handshake with the monitor and then sends heartbeats to that monitor.


## Monitor Node
- When this node becomes a monitor it first tries to connect with other monitors and waits for 5 consecutive heartbeats for their responses.
- If no monitor responds, this monitor will then assume the system is starting from scratch and will listen to any incoming request from any node. It notes down the ip of the sender and the role listed in the packet. It then gets the minimum constraints from its command line arguments.
- It first creates the minimum number of assigners, then the minimum number of workers and then the minimum number of monitors (excluding itself).
- If monitors do respond, this monitor assumes it was promoted and would request information from other nodes. And then keep its lifecycle running.
- It keeps listening other nodes' heartbeats and sends its own heartbeats. Its own heartbeat consists of all nodes in the network with their IP and the PSIs of the worker nodes.
- It listens for requests from the gateway and replies with the least occupied assigner's IP
- It answers nodes' questions if it has less than 10 nodes already with it (this can be adjusted using the command line arguments).


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
