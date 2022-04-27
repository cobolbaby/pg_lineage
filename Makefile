# This is how we want to name the binary output
release=dist/lineage
image=infra/lineage
gover=1.17.9
tag=1.0.1

build:
	make test
	make clean
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o $(release)_linux_amd64 .
	
clean:
	rm -rf $(release)*

test:
	go mod tidy
	go mod download
	go test -v ./...
	
docker:
	docker build --rm -t ${image}:${tag} --build-arg gover=${gover} .
