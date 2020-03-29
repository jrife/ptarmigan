.PHONY: proto

proto:
	docker build -t ptarmigan-protogen -f ./build/Dockerfile.protogen .
	docker run --rm -v /etc/passwd:/etc/passwd -u $(shell id -u):$(shell id -g) -v $(shell pwd):/home/me/ptarmigan ptarmigan-protogen /home/me/ptarmigan/build/generate-proto.sh