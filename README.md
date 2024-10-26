# KegDB

KegDB is a Java implementation of a key-value store based on the Bitcask paper ([Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)). It implements a Redis-compatible protocol (RESP) for client communication.

## Features

- Redis-compatible protocol (RESP)
- Persistent storage using log-structured file format
- Concurrent access support
- Data integrity verification using checksums
- Configurable settings via properties file

## Supported Commands

- `PING` - Test server connectivity
- `GET key` - Retrieve a value by key
- `SET key value` - Store a key-value pair
- `DEL key` OR `DELETE key` - Delete a key-value pair

## Configuration

Configuration is managed through `src/main/resources/db.properties`:
```properties
port=6379 # Server port (default: 6379)
thread_pool_size=10 # Number of worker threads
data_directory=./db # Data storage directory
max_file_size=10000000 # Maximum size for data files (10MB)
```

## Building and Running
### Prerequisites

- Java 17 or higher
- Maven

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/kegdb.jar
```
## Implementation Details

KegDB implements the Bitcask design with the following components:

- **KeyDir**: In-memory hash table mapping keys to file locations
- **Data Files**: Append-only log files storing the actual data
- **RESP Protocol**: Redis-compatible communication protocol
- **Concurrent Access**: Thread-safe operations using read-write locks