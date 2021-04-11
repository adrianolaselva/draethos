GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=draethos
IMAGE_NAME='$(BINARY_NAME):$(release)'
all: clean deps build test run
docker-build:
	docker build --rm -f "Dockerfile" -t $(IMAGE_NAME) "." --build-arg VERSION=$(release)
clean: 
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
build: 
	$(GOBUILD) -v ./
test: 
	$(GOTEST) -v ./...
run:
	./$(BINARY_NAME)
deps:
	$(GOGET) -d -v ./...
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_NAME) -v
