# Running the Multipaxos Implementation

## Prerequisites
- Go installed in system
- Ports 8001-8005 available on localhost

## Setup Instructions

### 1. Configure File Paths

Before running, update the following paths in your code:

**Server logging path:**
- File: `server/logging_helpers.go`
- Update the logging directory path to your local setup

**Client logging path:**
- File: `client_new/main.go` (line 328)
- Update the path to point to your server logs directory

### 2. Compile the Server

From the project root directory:

```bash
go build -o bin/mpaxos ./server
```

### 3. Start the Servers

Open 5 separate terminal windows and start each server:

**Terminal 1 - Server n1:**
```bash
./bin/mpaxos -id=1 -port=8001 -liveness=800 -peers=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005
```

**Terminal 2 - Server n2:**
```bash
./bin/mpaxos -id=2 -port=8002 -liveness=850 -peers=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005
```

**Terminal 3 - Server n3:**
```bash
./bin/mpaxos -id=3 -port=8003 -liveness=870 -peers=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005
```

**Terminal 4 - Server n4:**
```bash
./bin/mpaxos -id=4 -port=8004 -liveness=900 -peers=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005
```

**Terminal 5 - Server n5:**
```bash
./bin/mpaxos -id=5 -port=8005 -liveness=920 -peers=localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005
```

**Note:** Ensure all 5 servers are running before starting the client.

### 4. Verify Ports are Available

Check if ports are available:

**Linux/Mac:**
```bash
lsof -i :8001-8005
```

**Windows:**
```bash
netstat -an | findstr "800[1-5]"
```

If ports are in use, kill the processes or choose different ports.

### 5. Run the Client

From the project root directory:

```bash
go run ./client_new <path_to_csv_file>
```

**Example:**
```bash
go run ./client_new transactions.csv
```

## CSV File Format

Create a CSV file with the following structure:

```csv
Set Number,Transactions,Live Nodes
1,"(A, J, 3)","[n1, n2, n3, n4, n5]"
,"(C, D, 1)",
,"(B, E, 4)",
,"(F, G, 2)",
2,"(B, I, 2)","[n1, n2, n3, n4]"
,"(J, C, 4)",
,"LF",
3,"(B, F, 3)","[n1, n3, n5]"
,"(C, A, 2)",
```

### CSV Column Descriptions:

- **Set Number**: Integer identifying the transaction set (only on first row of each set)
- **Transactions**: 
  - Format: `(From, To, Amount)` where From/To are account names (A-J) and Amount is integer
  - Special value: `LF` triggers leader failure
- **Live Nodes**: Array of active nodes for this set (format: `[n1, n2, n3, n4, n5]`)

### CSV Rules:

1. First row must be the header: `Set Number,Transactions,Live Nodes`
2. Set number appears only on the first transaction of each set
3. Subsequent transactions in the same set leave Set Number and Live Nodes empty
4. All transactions must be enclosed in double quotes
5. Live nodes array must be enclosed in double quotes

## Client Commands

Once the client is running, you can use these interactive commands between sets:

- **`continue`** - Proceed to the next transaction set
- **`PrintDb <nodeID>`** - Display database state for a specific node
  - Example: `PrintDb n1`
- **`PrintLog <nodeID>`** - Display log entries for a specific node
  - Example: `PrintLog n2`
- **`PrintStatus <sequence>`** - Show transaction status across all nodes
  - Example: `PrintStatus 5`
- **`PrintView`** - Display all leader election history

## Example Session

```bash
$ go run ./client_new transactions.csv

2025/10/12 10:00:00 Client initialized with leader: n1
2025/10/12 10:00:00 Loaded 3 transaction sets

=== Processing Set 1 ===
2025/10/12 10:00:00 Live nodes: [n1 n2 n3 n4 n5]
2025/10/12 10:00:01 [+] Transaction A -> J (3) succeeded
2025/10/12 10:00:01 [+] Transaction C -> D (1) succeeded
2025/10/12 10:00:01 [+] Transaction B -> E (4) succeeded
2025/10/12 10:00:01 Set 1 completed. Failed: 0

Enter command (continue/PrintDb/PrintLog/PrintStatus/PrintView): PrintDb n1

=== Database for Node n1 ===
Account | Balance
--------|--------
A       | 97
B       | 96
C       | 99
D       | 101
E       | 104
F       | 98
G       | 100
H       | 100
I       | 100
J       | 103
=== End of Database ===

Enter command (continue/PrintDb/PrintLog/PrintStatus/PrintView): continue

=== Processing Set 2 ===
...
```

## Troubleshooting

### Common Issues:

**"Connection refused" error:**
- Verify all 5 servers are running
- Check that ports 8001-8005 are not blocked by firewall
- Confirm server processes with: `ps aux | grep mpaxos`

**"File not found" error for PrintDb/PrintLog:**
- Verify the paths in `client_new/main.go` line 328
- Ensure server has created log/db files in the correct directory
- Check file permissions


## Project Structure

```
project/
├── server/              # Server implementation
│   ├── logging_helpers.go
│   ├── main.go
│   └── ...
├── client_new/          # Client implementation
│   └── main.go
├── bin/                 # Compiled binaries
│   └── mpaxos
├── transactions.csv     # Transaction input file
└── README.md           # This file
```

## Implementation Features

### Client Features:
- **Set-by-set execution** with user interaction between sets
- **Per-user sequential execution** while parallelizing across different users
- **Leader discovery** with timeout-based broadcasting
- **Automatic retry** up to 10 times with exponential backoff
- **Failed transaction queuing** to next set
- **Leader failure simulation** (LF command)
- **Node configuration** updates between sets

### Server Features:
- **Multi-Paxos consensus** protocol implementation
- **Leader election** with configurable liveness timeouts
- **Persistent logging** for request metadata
- **Key-value datastore** for account balances
- **Transaction status tracking** (Accepted/Committed/Executed)
- **View change tracking** for observability

## Stopping the System

To shut down:

1. Press `Ctrl+C` in the client terminal
2. Press `Ctrl+C` in each of the 5 server terminals

## Testing

Use CSV test cases to test the project

## Authors

Pranav Pateriya  
CSE535 - Distributed Systems  
Stony Brook University  
Fall 2025



