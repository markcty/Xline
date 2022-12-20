sudo apt install -y cmake g++
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-linux-x86_64.zip
unzip protoc-21.10-linux-x86_64.zip -d .local
echo "$(pwd)/.local/bin" >> "$GITHUB_PATH"