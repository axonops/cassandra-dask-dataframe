# 🐛 Debug Statements to Clean Up

The following debug print statements were found during code analysis and should be removed before the 1.0 release:

## Files with Debug Statements

### 1. **partition_reader.py**
- Line 135: `# logger.debug(f"Created session for worker in {time.time() - start:.2f}s")`
- Line 137: `# print(f"Worker {worker_id} created session in {time.time() - start:.2f}s")`

### 2. **partition.py**
- Line 393: `print("DEBUG: Taking non-token range path")`
- Line 473: `print(f"DEBUG: First row from stream: {row}")`
- Line 475: `print(f"DEBUG: First row fields from query: {row._fields}")`
- Line 476: `print(f"DEBUG: Row values: {[getattr(row, f) for f in row._fields]}")`

### 3. **incremental_builder.py**
- Line 79: `# logger.debug(f"Created DataFrame with {len(df)} rows, memory: {memory_usage / 1024 / 1024:.2f} MB")`

### 4. **reader.py**
- Line 654-656: Debug comments that can be removed

## Recommendation

Before the 1.0 release:
1. Remove all print statements
2. Convert useful debug information to proper logger.debug() calls
3. Ensure all logging uses appropriate levels (DEBUG, INFO, WARNING, ERROR)
4. Add a logging configuration example to documentation

## Example Fix

Replace:
```python
print(f"DEBUG: First row from stream: {row}")
```

With:
```python
logger.debug(f"First row from stream: {row}")
```

Or remove entirely if not needed for production debugging.
