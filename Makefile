t:
	DB_TYPE=redis go test . -v
	DB_TYPE=sqlite go test . -v

f:
	go fmt ./...
