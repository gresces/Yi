.PHONY: build test ts-test ts-e2e rust-test rust-e2e android race bench stress logic fmt

BASE_URL ?= http://127.0.0.1:8081
FRONTEND_URL ?= http://127.0.0.1:8080
TOKEN ?=

build:
	go build -o dist/yi ./src

fmt:
	gofmt -w src

test:
	go test ./...
	cd lib/python && python3 -m unittest discover -s tests -t .

ts-test:
	node --test lib/ts/tests/unit.test.mjs

# 需要先启动服务,默认 :8081
ts-e2e:
	YI_BASE_URL=$(BASE_URL) YI_TOKEN=$(TOKEN) node --test lib/ts/tests/e2e.test.mjs

rust-test:
	cargo test --manifest-path lib/rust/Cargo.toml

# 需要先启动服务,默认 :8081
rust-e2e:
	YI_BASE_URL=$(BASE_URL) YI_TOKEN=$(TOKEN) \
		cargo test --manifest-path lib/rust/Cargo.toml live_e2e -- --nocapture

# 需要 Android SDK Platform 34 + Build Tools 35
android:
	./app/build.sh

race:
	go test -race ./...

bench:
	go test -bench . -benchmem -benchtime=1s ./src/bus

# 需要先启动服务,默认 :8081
stress:
	python3 lib/python/stress/stress.py \
		--base-url $(BASE_URL) --token "$(TOKEN)" \
		--producers 8 --consumers 4 --events-per-producer 500 \
		--multi-type-rate 0.2 --duplicate-every 50 --nack-rate 0.01

# 需要空数据库和前端端口,默认 :8080 / :8081
logic:
	python3 lib/python/stress/logic_check.py \
		--base-url $(BASE_URL) --token "$(TOKEN)" \
		--frontend-url $(FRONTEND_URL)
