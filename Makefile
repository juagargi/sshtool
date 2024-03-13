.PHONY: all build clean


all: build

build:
	@go build -o ./bin/ .

clean:
	@rm ./bin/sshtool
