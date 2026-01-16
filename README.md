# GSHARE

Fast & secure P2P file sharing across the globe.

Inspired by an earlier project of mine,
[yafs](https://github.com/Logan-010/yafs).

# How does it work?

Peer A (A) wants to share a file to Peer B (B). A runs gshare in share mode by
typing `gshare share message.txt`. A adds any local peers found by mDNS to the
DHT, connects to the IPFS network (if possible, fully offline/local discovery is
fully supported), and marks themselves as a provider of a uniquely generated
code. B now runs `gshare download <CODE>` and their computer searches the local
& IPFS DHT for any matches. B now connects to A either directly or via a relay
(you don't need your own! IPFS provides one) and A sends the file.
