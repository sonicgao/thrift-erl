THRIFT_FILES =  $(wildcard test/*.thrift)
THRIFT = thrift

.PHONY: test .generated clean

.generated: $(THRIFT_FILES)
	for f in $(THRIFT_FILES) ; do \
	  $(THRIFT) --gen erl -out test $$f ; \
	done ; \

all: .generated
	./rebar compile

test: .generated
	./rebar eunit

clean:
	rm -rf ebin
