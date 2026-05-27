1. in consensus.c I am assuming I will get a consensusCancel packet if some nodes hear the monitor, i have to yet implement that in the normal flow of the node.

2. In heartbeats.c, in the analyseHeartbeats function I am putting static port 8000 instead of having it dynamically.

3. In packets.h I have made the capacity of monitor a constant, it is also constant in Monitor.c file. currently both are same.

4. many many assumptions in consensus.c

