#!/bin/bash

# Exit on error
set -e

# Build the project
mvn clean install

# Find the jar file with dependencies
JAR_FILE=$(find target -name "tinkerbench-*-jar-with-dependencies.jar")
if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find the built JAR file"
    exit 1
fi

# Extract just the filename
JAR_NAME=$(basename "$JAR_FILE")

# Create bin directory in the project
mkdir -p bin

# Create the wrapper script
cat > bin/tinkerbench2 << EOF
#!/bin/sh
SCRIPT_DIR="\$(cd "\$(dirname "\$0")" && pwd)"
java -jar "\$SCRIPT_DIR/../target/$JAR_NAME" "\$@"
EOF

# Make the wrapper script executable
chmod +x bin/tinkerbench

echo "Installation complete!"
echo "You can now run the tool using: ./bin/tinkerbench"
