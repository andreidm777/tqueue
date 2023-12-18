.PHONY: test

test:
	for f in `ls t/*`; do \
	   tarantool $$f; \
	done