# Zookeeper - Map Reduce

## 1. Introduction

This is the final lab project focused on **distributed systems**, where ZooKeeper is used to coordinate and manage distributed components. It describes the activities needed to:


- Build a ZooKeeper *ensemble*.
- Run example programs that exercise its functionalities.

## 2. Setting up a ZooKeeper Ensemble with Docker

The recommended way to set up the ensemble is using **Docker**, with the official ZooKeeper image.

### 2.1 Standalone Ensemble (Single Server)

To start a single-server ensemble:

```bash
docker run -p 2181:2181 -p 8880:8080 --name zoo1 --restart always -d zookeeper
```

- **2181:** Port for client connections.  
- **8880:** Administration port (optional for this lab).

**Check server status inside the container:**

```bash
docker exec -it zoo1 bash
zkServer.sh status
# Should display: 'Mode: standalone'
```

**Connect local client:**

```bash
zkCli.sh -server localhost:2181
```

### 2.2 Quorum Ensemble (Multiple Local Servers)

For a *quorum* system (several servers replicating data):

- Use **Docker Compose** with three servers: `zoo1`, `zoo2`, and `zoo3`.
- Key environment variables:

  - `ZOO_MY_ID`: Unique ID for each server (1, 2, 3).  
  - `ZOO_SERVERS`: List of all servers, including quorum ports (2888) and leader election ports (3888).  
    Example for `zoo1`:
    ```
    server.1=zoo1:2888:3888;2181
    server.2=zoo2:2888:3888;2181
    server.3=zoo3:2888:3888;2181
    ```

## 3. Local Installation of ZooKeeper

To use `zkCli.sh` and other management tools:

1. **Download and create directory:**

```bash
mkdir ~/zookeeper
```

2. **Extract the package (available on Moodle):**

```bash
cd ~/zookeeper
tar xvfz /path/to/apache-zookeeper-3.9.2-bin.tgz
```

3. **Set environment variables:**

```bash
export CLASSPATH=$CLASSPATH:~/zookeeper/apache-zookeeper-3.9.2-bin/lib/*
export PATH=$PATH:~/zookeeper/apache-zookeeper-3.9.2-bin/bin
```

## 4. Implemented Functionalities

The project implements the following core distributed functionalities using ZooKeeper:

### 4.1 Leader Election

- Distributed program to elect a leader among correct servers.  
- Features:

  - Consensus among all servers.  
  - Leader remains constant until failure.  
  - Based on the concept of *member nodes* in ZooKeeper.

### 4.2 Counter Consistency

- Distributed system where multiple applications share a **counter** stored in a ZooKeeper zNode.  
- Implementation options:

  1. **Versioning (Busy Waiting):** Get counter and version; update with retries in case of conflicts.  
  2. **Distributed Lock:** Ensure only one application updates the counter at a time.

- The final counter value is verified after many concurrent increments to ensure correctness.
