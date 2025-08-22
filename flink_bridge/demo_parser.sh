#!/bin/bash

echo "=== Pipeline Parser Demo ==="
echo ""

# Compile the project
echo "1. Compiling the project..."
mvn compile -q
if [ $? -eq 0 ]; then
    echo "   ✓ Compilation successful"
else
    echo "   ✗ Compilation failed"
    exit 1
fi

echo ""

# Run the tests
echo "2. Running tests..."
mvn test -Dtest=PipelineParserTest -q
if [ $? -eq 0 ]; then
    echo "   ✓ All tests passed"
else
    echo "   ✗ Some tests failed"
fi

echo ""

# Show the example YAML file
echo "3. Example YAML file (example_pipeline.yaml):"
cat example_pipeline.yaml

echo ""

# Show how to run the example
echo "4. To run the parser example:"
echo "   mvn exec:java -Dexec.mainClass=\"io.sentry.flink_bridge.PipelineParserExample\""
echo ""
echo "   Or compile and run manually:"
echo "   mvn package"
echo "   java -cp target/flink-bridge-app.jar io.sentry.flink_bridge.PipelineParserExample"

echo ""
echo "=== Demo Complete ==="
