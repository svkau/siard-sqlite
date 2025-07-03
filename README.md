# SIARD to SQLite Converter

A Python tool for converting SIARD (Software Independent Archival of Relational Databases) archive files to SQLite databases for analysis and exploration.

## Overview

SIARD files are ZIP archives containing database structure metadata (XML) and data (XML) following the SIARD 2.1 specification. This tool extracts and converts them to SQLite format, making the data easily accessible for analysis, exploration, and integration with modern data tools.

## Features

- **Schema conversion** - Tables, columns, data types, primary keys, and foreign keys
- **Views support** - Converts SIARD views to SQLite views with query translation
- **Streaming support** - Processes large files (>50MB) using streaming XML parser
- **Type mapping** - Conversion from SIARD SQL types to SQLite types
- **Batch processing** - Optimized data import with configurable batch sizes
- **Progress monitoring** - Real-time import progress for large datasets (not implemented)
- **CLI** - Multiple command options
- **Error handling** - Comprehensive error reporting and recovery

## Installation

### From PyPI (Recommended)

```bash
# Install with pip
pip install siard-sqlite

# Or with uv (faster)
uv add siard-sqlite
```

### From Source

```bash
# Clone the repository
git clone https://github.com/svkau/siard-sqlite.git
cd siard-sqlite

# Install with uv
uv sync

# Or with pip
pip install -e .
```

### Prerequisites

- Python 3.12+

## Usage

### Basic Usage

```bash
# Using the CLI commands (recommended)
siard-convert input.siard output.sqlite
siard2sqlite input.siard output.sqlite

# With verbose logging
siard-convert --verbose input.siard output.sqlite

# Quiet mode (only errors)
siard-convert --quiet input.siard output.sqlite

# Traditional Python script method
python siard_converter.py input.siard output.sqlite
uv run siard_converter.py input.siard output.sqlite
```

### Command Line Options

```
positional arguments:
  siard_file            Path to SIARD file
  sqlite_file           Output SQLite file path

options:
  -h, --help            Show help message and exit
  -v, --verbose         Enable verbose logging (shows detailed progress)
  -q, --quiet           Suppress all output except errors
  --no-foreign-keys     Skip creating foreign key constraints
  --no-views            Skip creating views
  --batch-size SIZE     Batch size for data import (default: 1000)
  --streaming-threshold MB
                        File size threshold in MB for streaming parser (default: 50)
  --version             Show program version and exit
```

### Examples

```bash
# Basic conversion
siard-convert employees.siard employees.db

# Advanced conversion with custom settings
siard-convert --verbose --batch-size 5000 --streaming-threshold 100 large_data.siard output.db

# Skip foreign keys and views for faster conversion
siard-convert --no-foreign-keys --no-views --quiet archive.siard simple.db

# Explore the converted data
datasette employees.db
sqlite3 employees.db "SELECT COUNT(*) FROM employees;"
```

## SIARD File Structure

SIARD archives contain:
- `header/metadata.xml` - Database schema definition with tables, columns, and constraints
- `content/{schema}/{table}.xml` - Table data files using SIARD's XML format
- Data uses `<row><c1>value1</c1><c2>value2</c2></row>` structure

## Architecture

### Core Components

- **`SiardToSqlite`** - Main converter class handling the complete conversion pipeline
- **Metadata Parser** - XML parsing with namespace fallback strategies  
- **Type Mapper** - Converts SIARD SQL types to appropriate SQLite types
- **Data Importer** - Handles both regular and streaming XML parsing for optimal performance

### Conversion Pipeline

1. **Extract** - Unzip SIARD archive to temporary directory
2. **Parse Metadata** - Read schema definitions from `metadata.xml`
3. **Create SQLite Schema** - Map types and create tables with constraints
4. **Import Data** - Parse XML data files and insert into SQLite (with streaming for large files)

### Performance Features

- **Streaming Parser** - Automatically used for files >50MB to handle large datasets efficiently
- **Batch Processing** - Configurable batch sizes (default: 1000 rows) for optimal import speed
- **Memory Management** - Automatic cleanup of parsed XML elements to prevent memory issues
- **Progress Monitoring** - Progress updates every 10,000 rows for long-running imports

## Configuration

Key configuration constants (can be modified in the code):

```python
STREAMING_THRESHOLD_MB = 50    # File size threshold for streaming parser
BATCH_SIZE = 1000             # Batch size for data import
PROGRESS_INTERVAL = 10000     # Progress update interval
```

## Data Type Mapping

| SIARD Type | SQLite Type |
|------------|-------------|
| CHARACTER, VARCHAR, CHAR, TEXT, CLOB | TEXT |
| INTEGER, INT, BIGINT, SMALLINT, TINYINT | INTEGER |
| DECIMAL, NUMERIC, FLOAT, DOUBLE, REAL | REAL |
| DATE, TIME, TIMESTAMP, DATETIME | TEXT |
| BOOLEAN, BOOL | INTEGER |
| BLOB, BINARY, VARBINARY | BLOB |

## Testing

### Create Test Data

Use the included test data generator:

```bash
# Create a sample SIARD file for testing
python create_test_siard.py -o test.siard

# Convert the test file
python siard_converter.py test.siard test.sqlite
```

### Debug SIARD Files

Analyze SIARD file structure:

```bash
# Examine SIARD file contents and structure
python debug_siard.py input.siard
```

## Troubleshooting

### Common Issues

**Empty column warnings**: These are normal for SIARD files containing placeholder elements and can be ignored.

**Namespace errors**: The converter uses multiple fallback strategies for XML namespace handling.

**Large file memory issues**: Files >50MB automatically use streaming parser to prevent memory problems.

**File path issues**: The converter tries multiple naming conventions:
- Generic names: `table1.xml`, `table2.xml`
- Named files: `tablename.xml`
- Subdirectories: `table1/table1.xml`

### Verbose Logging

For detailed troubleshooting, use verbose mode:

```bash
uv run siard_converter.py input.siard output.sqlite -v
```

This shows:
- XML parsing details
- File path resolution attempts
- Import progress and statistics
- Any warnings or errors

## Dependencies

- **lxml** - Fast XML parsing with XPath support
- **Python 3.12+** - Modern Python with type hints and performance improvements

See `pyproject.toml` for complete dependency list.

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]

## Supported SIARD Versions

- SIARD 2.1 specification
- Various namespace configurations
- Multiple XML schema variations

## Performance

Tested with:
-  Small databases: <1MB, instant conversion
-  Medium databases: 10-100MB, seconds to minutes
-  Large databases: >200MB, automatic streaming with progress monitoring
-  Very large datasets: 2.8M+ rows successfully imported

Example performance (employees database):
- 6 tables, 3.9M total rows
- salaries table: 2.8M rows, 210MB XML file
- Total conversion time: ~5 minutes
- Memory usage: <200MB (thanks to streaming)