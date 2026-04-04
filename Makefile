.PHONY: build build-small clean

OUTPUT = ./dist/chb

build:
	@mkdir -p ./dist
	go build -o $(OUTPUT) .

build-small:
	@mkdir -p ./dist
	go build -ldflags="-s -w" -trimpath -o $(OUTPUT) .

clean:
	rm -f $(OUTPUT)
