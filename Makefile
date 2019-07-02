build:
	mkdir -p bin/
	go build -o bin/teampulse-server
	chmod +x bin/teampulse-server

run: build
	bin/teampulse-server

test:
	go test ./...
