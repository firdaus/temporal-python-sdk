rm -rf temporal-api
git clone https://github.com/temporalio/api.git temporal-api
cd temporal-api
python -m grpc_tools.protoc -I . --python_betterproto_out=.. `find . -name '*.proto'`
