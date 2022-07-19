build-and-push: build push
	echo "Building and pushing..."

build:
	docker build . \
		--progress=plain \
		--tag 615645230945.dkr.ecr.ap-southeast-2.amazonaws.com/xbt-lambda:latest

push:
	docker push \
		615645230945.dkr.ecr.ap-southeast-2.amazonaws.com/xbt-lambda:latest
