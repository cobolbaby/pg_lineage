# This is how we want to name the binary output
release=dist/pg_lineage
image=infra/pg_lineage
gover=1.17.9
tag=1.0.1
proxy=10.190.40.239:3389

build:
	make test
	make clean
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o $(release)_linux_amd64 .
	
clean:
	rm -rf $(release)*

test:
	go mod tidy
	# go mod download
	go test -v ./...
	
docker:
	docker build --rm -t ${image}:${tag} \
		--build-arg http_proxy=http://${proxy} \
		--build-arg https_proxy=http://${proxy} \
		--build-arg no_proxy=*.inventec.net \
		--build-arg gover=${gover} \
		.
