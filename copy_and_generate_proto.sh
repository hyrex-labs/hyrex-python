#!/bin/bash
set -e

# Parse CLI flag for proto source repo path
proto_source=""
[[ "$1" == "--proto_source" ]] && proto_source="$2"

# Check if proto_source is set
if [ -n "$proto_source" ]; then
  echo "Proto source repo: $proto_source"
else
  echo "Error: --proto_source flag is required" >&2
  exit 1
fi

LOCAL_PROTO_DIR=hyrex/proto

# Remove existing files
rm -rf ${LOCAL_PROTO_DIR}

mkdir -p ${LOCAL_PROTO_DIR}
touch ${LOCAL_PROTO_DIR}/__init__.py # Ensure it's a package

# Copy required performanceserver protos into local proto dir
cp ${proto_source}/performanceserver/task.proto ${LOCAL_PROTO_DIR}/
cp ${proto_source}/performanceserver/gateway.proto ${LOCAL_PROTO_DIR}/
cp ${proto_source}/performanceserver/requests.proto ${LOCAL_PROTO_DIR}/

echo "Running protoc..."
python -m grpc_tools.protoc \
  --proto_path=${LOCAL_PROTO_DIR} \
  --python_out=${LOCAL_PROTO_DIR} \
  --grpc_python_out=${LOCAL_PROTO_DIR} \
  ${LOCAL_PROTO_DIR}/*.proto

echo "Initial Python code generated under ./${LOCAL_PROTO_DIR}/"

# --- Post-Processing Step ---
echo "Applying relative import fix..."

find "${LOCAL_PROTO_DIR}" -name '*.py' -print0 | while IFS= read -r -d $'\0' file; do
      # Uses -i '' for in-place edit without backup on macOS/BSD
      sed -i '' 's/^import \([a-zA-Z0-9_]*_pb2\)\(.*\)/from . import \1\2/' "$file"
    done

echo "Relative import fix applied."
echo "Final Python code ready under ./${LOCAL_PROTO_DIR}/"
