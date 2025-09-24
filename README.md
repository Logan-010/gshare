# GSHARE
Fast & secure P2P file sharing across the globe.

Inspired by an earlier project of mine, [yafs](https://github.com/Logan-010/yafs).

# How does it work?
## Relay connections:
Peer A (A) wants to share a file to Peer B (B). A runs gshare in share mode by typing `gshare share message.txt`. A sends a request to the libp2p bootstrap server to make a relay reservation. A then copies the command printed from gshare and sends it to B somehow (this part is on YOU!). B runs `gshare download <TICKET>` and their computer makes a request to connect to A through the relay using information encoded in the ticket. The relay then **attempts** to connect the two peers directly. A is now connected to B and sends the file!

## Local connections:
Peer A (A) wants to share a file to Peer B (B). A runs gshare in (local) share mode by typing `gshare --local share message.txt`. A then advertises themself over their local network. A then copies the command printed from gshare and sends it to B somehow (this part is again, on YOU!). B runs `gshare --local download <TICKET>` and their computer searches for reachable peers on their local network. Peer B connects to peer A and A sends the file.