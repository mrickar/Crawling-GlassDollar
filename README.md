# Crawling GlassDollar

- This project is an API service running on Docker container on port 8000.
- It will return 848 enterprises and their details.
## Prerequisites

Ensure you have the following installed before setting up the project:

- Docker Compose ->

   used version = Docker Compose version v2.23.3-desktop.2


## Getting Started

Follow these steps to set up and run the project locally.

### Installation

```bash
# Clone the repository
git clone https://github.com/mrickar/Crawling-GlassDollar

# Change into the project directory
cd Crawling-GlassDollar

# For building the first time
# Build docker containers
docker compose up --build 
#or docker-compose up --build

#For the rest of the runs
#docker compose up

```
## How to use

- After the project is built and run successfully on docker. You can use the following endpoint to get your data.<br>
http://127.0.0.1:8000/enterprises/

- After the request is sent, the response will provide a new endpoint to see the results of the async process.<br>
http://127.0.0.1:8000/requests/{task_id} <br>
task_id will be assigned by service.

- The page will display a "Task in progress" message until the process is done and the results are ready. After the process finishes, you can see the results by refreshing the page.
  
- Celery tasks can be monitored with Flower<br>
http://127.0.0.1:5556/
