.PHONY: proto

proto:
	docker build -t flock-protogen -f ./build/Dockerfile.protogen .
	docker run --rm -v /etc/passwd:/etc/passwd -u $(shell id -u):$(shell id -g) -v $(shell pwd):/home/me/flock flock-protogen /home/me/flock/build/generate-proto.sh