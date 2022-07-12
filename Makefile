GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=draethos
IMAGE_NAME=$(BINARY_NAME):$(release)
all: clean deps build test run

ifndef tag
override tag = $(IMAGE_NAME)
endif

ifndef release
override release = latest
endif

ifndef platform
override platform = linux/amd64
endif

docker-build:
	docker build --rm -f "Dockerfile" -t $(tag) "." --build-arg VERSION=$(release) --platform=$(platform)
clean: 
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
build: 
	$(GOBUILD) -o $(BINARY_NAME) -v ./init
test: 
	$(GOTEST) -v ./...
run:
	$(GOCMD) run ./...
deps:
	$(GOGET) -d -v ./...
build-linux:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_NAME) -v ./init