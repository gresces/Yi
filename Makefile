.PHONY: build test race bench stress logic fmt

build:
	go build -o dist/yi ./src

fmt:
	gofmt -w src

test:
	go test ./...
	cd lib/python && python3 -m unittest discover -s tests -t .

race:
	go test -race ./...

bench:
	go test -bench . -benchmem -benchtime=1s ./src/bus

# 需要先启动服务,默认 :8081
stress:
	python3 lib/python/stress/stress.py \
		--base-url http://127.0.0.1:8081 \
		--producers 8 --consumers 4 --events-per-producer 500 \
		--multi-type-rate 0.2 --duplicate-every 50 --nack-rate 0.01

# 需要空数据库和前端端口,默认 :8080 / :8081
logic:
	python3 lib/python/stress/logic_check.py \
		--base-url http://127.0.0.1:8081 \
		--frontend-url http://127.0.0.1:8080
