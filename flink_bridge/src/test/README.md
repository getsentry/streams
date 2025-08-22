# Testing Structure

This directory contains the unit tests for the Flink gRPC Bridge project.

## Test Structure

```
src/test/
├── java/
│   └── com/
│       └── sentry/
│           └── flink_bridge/
│               └── BasicTest.java          # Basic test examples + GrpcClient mock tests
└── README.md                               # This file
```

## Running Tests

### Using Maven

```bash
# Run all tests
mvn test

# Run only test compilation
mvn test-compile

# Run tests with verbose output
mvn test -X

# Run a specific test class
mvn test -Dtest=BasicTest

# Run a specific test method
mvn test -Dtest=BasicTest#testBasicAssertion
```

### Test Dependencies

The project uses:
- **JUnit 5** (`junit-jupiter`) for test framework
- **Maven Surefire Plugin** for test execution

## Writing Tests

### Basic Test Structure

```java
package com.sentry.flink_bridge;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MyTest {

    @Test
    void testSomething() {
        // Arrange
        String expected = "Hello";

        // Act
        String actual = "Hello";

        // Assert
        assertEquals(expected, actual, "Strings should be equal");
    }
}
```

### Test Naming Convention

- Test classes: `*Test.java`
- Test methods: `test*()` or descriptive names
- Use descriptive test names that explain what is being tested

### Assertions

Common JUnit 5 assertions:
- `assertEquals(expected, actual, message)`
- `assertTrue(condition, message)`
- `assertFalse(condition, message)`
- `assertNotNull(object, message)`
- `assertNull(object, message)`
- `assertThrows(ExceptionClass.class, () -> code)`

## Test Coverage

Currently implemented:
- ✅ Basic test infrastructure
- ✅ Simple assertion tests (3 tests)
- ✅ GrpcClient mock tests (4 tests)
  - Success case with multiple messages
  - Empty response handling
  - Exception handling
  - Request construction verification

## GrpcClient Mock Testing

The `BasicTest` class includes a `MockGrpcClient` that simulates the behavior of the real `GrpcClient`:

### Features
- **Controlled Responses**: Set expected responses for testing different scenarios
- **Exception Testing**: Simulate gRPC errors and exceptions
- **Request Verification**: Inspect the requests sent to verify correct construction
- **No External Dependencies**: Tests run without requiring a real gRPC server

### Usage Example
```java
@Test
void testGrpcClientProcessMessage_Success() {
    // Arrange
    Message inputMessage = createTestMessage("test message");
    mockGrpcClient.setNextResponse(createMockResponse(
        createTestMessage("processed message 1"),
        createTestMessage("processed message 2")
    ));

    // Act
    List<Message> result = mockGrpcClient.processMessage(inputMessage);

    // Assert
    assertEquals(2, result.size());
    assertEquals("processed message 1", getMessagePayload(result.get(0)));
}
```

## Next Steps

1. **Integration Tests**: Add tests that verify the complete workflow
2. **More Unit Tests**: Test other classes and methods
3. **Test Utilities**: Create helper classes for common test scenarios
4. **Performance Tests**: Add tests for performance characteristics
