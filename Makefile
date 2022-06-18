CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem\
		-ca-key=ca-key.pem\
		-config=test/ca-config.json\
		-profile=server\
		test/server-csr.json | cfssljson -bare server
	cfssl gencert \
    		-ca=ca.pem\
    		-ca-key=ca-key.pem\
    		-config=test/ca-config.json\
    		-profile=client\
    		test/client-csr.json | cfssljson -bare client
	cfssl gencert \
    		-ca=ca.pem\
    		-ca-key=ca-key.pem\
    		-config=test/ca-config.json\
    		-profile=client\
    		-cn="root"\
    		test/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
    		-ca=ca.pem\
    		-ca-key=ca-key.pem\
    		-config=test/ca-config.json\
    		-profile=client\
    		-cn="nobody"\
    		test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	protoc --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        api/v1/log.proto

.PHONY: test
test:
	  cp test/policy.csv $(CONFIG_PATH)policy.csv && cp test/model.conf $(CONFIG_PATH)model.conf &&   go test -v ./...
