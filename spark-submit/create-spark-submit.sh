#!/bin/bash
# Script to create a self-extracting spark-submit executable

set -e

# Build the project first
echo "Building the project..."
mvn clean package -DskipTests

# Find the JAR file (prefer shaded/with-deps, skip the original-* JAR)
JAR_FILE=$(find target -maxdepth 1 \( -name "*-shaded.jar" -o -name "*-with-dependencies.jar" \) | head -n 1)
if [ -z "$JAR_FILE" ]; then
    JAR_FILE=$(find target -maxdepth 1 -name "*.jar" \
        ! -name "original-*.jar" \
        ! -name "*-sources.jar" \
        ! -name "*-javadoc.jar" | head -n 1)
fi

if [ -z "$JAR_FILE" ]; then
    echo "Error: Could not find JAR file in target directory"
    exit 1
fi

echo "Found JAR file: $JAR_FILE"

# Create the self-extracting script
OUTPUT_FILE="spark-submit"
cat > "$OUTPUT_FILE" << 'SCRIPT_END'
#!/bin/bash
# Self-extracting spark-submit wrapper

# Extract the JAR file
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

# Find the line number where the JAR starts
ARCHIVE_START=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "$0")

# Extract the JAR
tail -n +$ARCHIVE_START "$0" > "$TMP_DIR/app.jar"

# Run the application
java -jar "$TMP_DIR/app.jar" "$@"

exit $?

__ARCHIVE_BELOW__
SCRIPT_END

# Append the JAR file to the script
cat "$JAR_FILE" >> "$OUTPUT_FILE"

# Make it executable
chmod +x "$OUTPUT_FILE"

echo "Created self-extracting executable: $OUTPUT_FILE"
echo "You can now use ./spark-submit to submit Spark jobs to Kyuubi"


