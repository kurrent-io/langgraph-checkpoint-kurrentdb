# LangGraph Checkpoint KurrentDB

## Build and Test Commands
- Run all tests: `poetry run pytest`
- Run a specific test: `poetry run pytest tests/test_sync.py::test_function_name -v`
- Run tests with async support: `poetry run pytest --asyncio-mode=auto`
- Run tests with coverage: `poetry run pytest --cov=src/`
- Install project: `poetry install`
- Install with dev dependencies: `poetry install --with dev`
- Type check: `poetry run mypy src/ tests/`

## Test Organization
- **test_sync.py**: Basic synchronous functionality tests
- **test_advanced.py**: Tests for complex operations, advanced features, and async methods
- **test_persistence.py**: Tests for data persistence, multiple threads/namespaces, and state continuity
- **test_error_handling.py**: Tests for error conditions, exception handling, and edge cases

## Code Style Guidelines
- **Imports**: Group imports by stdlib, third-party, and local modules with a blank line between groups
- **Typing**: Use type annotations for all functions and methods; use generic types when appropriate
- **Variable Names**: Use snake_case for variables/functions, PascalCase for classes
- **Error Handling**: Use explicit exceptions with detailed error messages; catch specific exceptions
- **Documentation**: Add docstrings to all classes and public methods
- **Async Pattern**: Prefix async functions with 'a' (e.g., `async def aget_tuple()`)
- **Constants**: Use UPPER_CASE for constants
- **Testing**: Write unit tests for all public functionality; use pytest fixtures
- **Line Length**: Maximum 100 characters