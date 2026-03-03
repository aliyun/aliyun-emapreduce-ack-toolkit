#!/bin/bash
# Script to create self-extracting spark-submit and spark-sql executables

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

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

# Get JAR checksum for cache validation
JAR_CHECKSUM=$(md5 -q "$JAR_FILE" 2>/dev/null || md5sum "$JAR_FILE" | cut -d' ' -f1)
echo "JAR checksum: $JAR_CHECKSUM"

# Function to create self-extracting executable
create_executable() {
    local OUTPUT_FILE="$1"
    local EXTRA_ARGS="$2"
    local DESCRIPTION="$3"
    
    echo "Creating $OUTPUT_FILE ($DESCRIPTION)..."
    
    cat > "$OUTPUT_FILE" << SCRIPT_END
#!/bin/bash
# Self-extracting $OUTPUT_FILE wrapper
# Generated from aliyun-emapreduce-ack-toolkit
# Checksum: $JAR_CHECKSUM

# Cache directory for extracted JAR
CACHE_DIR="\${HOME}/.cache/emr-spark-tools"
CACHE_JAR="\${CACHE_DIR}/spark-submit-${JAR_CHECKSUM}.jar"

# Extract JAR to cache if not exists or checksum mismatch
extract_jar() {
    mkdir -p "\$CACHE_DIR"
    
    # Check if cached JAR exists and is valid
    if [ -f "\$CACHE_JAR" ]; then
        return 0
    fi
    
    # Clean old cached JARs
    rm -f "\${CACHE_DIR}"/spark-submit-*.jar 2>/dev/null || true
    
    # Find the line number where the JAR starts
    ARCHIVE_START=\$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "\$0")
    
    # Extract the JAR to cache
    tail -n +\$ARCHIVE_START "\$0" > "\$CACHE_JAR"
}

# Extract JAR (uses cache)
extract_jar

# Run the application
java \$JAVA_OPTS -jar "\$CACHE_JAR" $EXTRA_ARGS"\$@"

exit \$?

__ARCHIVE_BELOW__
SCRIPT_END

    # Append the JAR file to the script
    cat "$JAR_FILE" >> "$OUTPUT_FILE"
    
    # Make it executable
    chmod +x "$OUTPUT_FILE"
    
    echo "Created: $OUTPUT_FILE"
}

# Create spark-submit (general purpose)
create_executable "spark-submit" "" "Spark job submission tool"

# Create spark-sql (SQL mode shortcut)
create_executable "spark-sql" "" "Spark SQL execution tool"

echo ""
echo "=========================================="
echo "Build completed successfully!"
echo "=========================================="
echo ""
echo "Generated executables:"
echo "  ./spark-submit  - Submit Spark jobs (JAR/PySpark) or execute SQL"
echo "  ./spark-sql     - Shortcut for SQL execution"
echo ""
echo "Usage examples:"
echo ""
echo "  # Execute SQL inline"
echo "  ./spark-sql -e \"SHOW DATABASES\""
echo "  ./spark-submit -e \"SELECT * FROM my_table\""
echo ""
echo "  # Execute SQL from file"
echo "  ./spark-sql -f queries.sql"
echo ""
echo "  # Submit Spark JAR job"
echo "  ./spark-submit --class com.example.Main oss://bucket/app.jar"
echo ""
echo "  # With Kyuubi config via command line"
echo "  ./spark-submit --kyuubi-url http://localhost:10099 \\"
echo "                 --kyuubi-user user --kyuubi-password pwd \\"
echo "                 -e \"SHOW DATABASES\""
echo ""
echo "Installation:"
echo "  sudo cp spark-submit spark-sql /usr/local/bin/"
echo ""
