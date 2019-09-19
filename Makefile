all: node

node: src/node.go
	go build -o node src/node.go
