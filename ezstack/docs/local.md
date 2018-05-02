# Running Locally

EZstack is a designed as a distributed system, so running the system locally is not trivial. That being said, we have created a way to run EZstack using Docker for demo purposes.

### Build Docker Image

`docker build -t ezstack .`

### Run Docker Image

`docker run -it -p 8080:8080 --rm ezstack`

After running this command, EZstack will be available on port 8080!