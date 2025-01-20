# Task Orchestrator

## Overview

The Task Orchestrator is a task management system designed to handle task execution across worker nodes. It supports both **concurrent** and **sequential** task execution modes, leveraging Redis for synchronization and communication.

## Features

- **Concurrent Task Execution**: Tasks can run in parallel across multiple workers.
- **Sequential Task Execution**: Tasks in a group execute one at a time in a specific order.
- **Retry Mechanism**: Automatically retries failed tasks up to a configurable number of attempts with exponential backoff.
- **Worker Heartbeat**: Tracks worker health and reassigns tasks from inactive workers.
- **Graceful Shutdown**: Ensures in-progress tasks complete during shutdown.

---

## Requirements

- **Go**: Version 1.23
- **Redis**: Version 6 or higher
- **Docker**: For running Redis locally

---
Ensure the following are installed on your system:
1. **Docker**: You can download and install Docker from [Docker's official website](https://www.docker.com/products/docker-desktop).
2. **Docker Compose**: Docker Compose usually comes bundled with Docker Desktop. Verify your installation by running:
```sh
   docker-compose --version
```

## Setup Instructions

### Clone the Repository

```bash
  git clone https://github.com/mdshahjahanmiah/task-orchestrator.git
  cd task-orchestrator
```

## Run the Application
### Start the Orchestrator
    
```bash 
  go run cmd/main.go
```
### Check Task Status
#### Make the task_status.sh script executable

```bash 
  chmod +x task_status.sh
```
#### Run the script to check task states

```bash 
  ./task_status.sh
```
### Successful Script Execution
#### When the script runs successfully, the output will look like this:
```bash 
  Fetching Task States...
task-1: Success
task-2: Success
task-3: Success
task-4: Success
task-5: Failed
task-6: Running
task-7: Success
task-8: Pending
task-9: Failed
task-10: Pending

Fetching Task Retries...
task-1: Retries=1
task-9: Retries=2
task-4: Retries=1

Counting Task States...
Pending: 3
Running: 1
Success: 5
Failed: 2

Tasks Exceeding Retry Limit...

Script execution completed.
```
## Running Tests
```bash 
  make test
```

### Known Issues and Limitations
- **Sequential Task Blocking**: Sequential tasks in the same group block other tasks in that group until completed.
- **Potential for Optimization**: `main.go` can be optimized using `sync.WaitGroup` to manage goroutines efficiently.
- **Task Submission Flexibility**: Tasks are currently submitted on application start. Running the orchestrator standalone and enabling task submission via CLI or external requests would provide greater flexibility.
- **Environment-Based Configuration**: Configuration is not read from environment variables. Enhancing this to support environment variables or external files is recommended for production-ready deployment.
- **Combined Configuration Structure** The current configuration structure combines orchestrator and worker settings in the same struct. Separating them into distinct configurations would improve scalability and maintainability.
- **Logging**: Logging provides minimal information. Enhancing logging to include detailed context and diagnostics would improve debugging and monitoring.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Author
[Miah Md Shahjahan](https://smiah.dev)
