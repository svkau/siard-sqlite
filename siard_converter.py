#!/usr/bin/env python3
"""
SIARD to SQLite Converter
Converts SIARD archive files to SQLite databases for analysis and exploration.
"""

import zipfile
import sqlite3
import tempfile
import shutil
import re
from pathlib import Path
from lxml import etree
from typing import Dict, List, Optional, Tuple
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SiardToSqlite:
    """Converts SIARD files to SQLite databases."""

    # Configuration constants
    STREAMING_THRESHOLD_MB = 50
    BATCH_SIZE = 1000
    PROGRESS_INTERVAL = 10000

    # Simplified datatype mapping for analysis purposes
    TYPE_MAPPING = {
        'CHARACTER': 'TEXT',
        'VARCHAR': 'TEXT',
        'CHAR': 'TEXT',
        'TEXT': 'TEXT',
        'CLOB': 'TEXT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'INTEGER',
        'SMALLINT': 'INTEGER',
        'TINYINT': 'INTEGER',
        'DECIMAL': 'REAL',
        'NUMERIC': 'REAL',
        'FLOAT': 'REAL',
        'DOUBLE': 'REAL',
        'REAL': 'REAL',
        'DATE': 'TEXT',
        'TIME': 'TEXT',
        'TIMESTAMP': 'TEXT',
        'DATETIME': 'TEXT',
        'BOOLEAN': 'INTEGER',
        'BOOL': 'INTEGER',
        'BLOB': 'BLOB',
        'BINARY': 'BLOB',
        'VARBINARY': 'BLOB'
    }

    def __init__(self, siard_path: str, sqlite_path: str):
        self.siard_path = Path(siard_path)
        self.sqlite_path = Path(sqlite_path)
        self.temp_dir = None
        self.metadata = None
        self.schemas = []
        
        # Validate input
        self._validate_input()

    def _validate_input(self):
        """Validate input file paths and requirements."""
        if not self.siard_path.exists():
            raise FileNotFoundError(f"SIARD file not found: {self.siard_path}")
        
        if not self.siard_path.suffix.lower() == '.siard':
            raise ValueError(f"Input file must have .siard extension: {self.siard_path}")
        
        if self.sqlite_path.exists():
            logger.warning(f"Output file already exists and will be overwritten: {self.sqlite_path}")
        
        # Create parent directory if it doesn't exist
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    def convert(self):
        """Main conversion process."""
        try:
            logger.info(f"Converting {self.siard_path} to {self.sqlite_path}")

            # Extract SIARD archive
            self._extract_siard()

            # Parse metadata
            self._parse_metadata()

            # Create SQLite database
            self._create_sqlite_database()

            # Import data
            self._import_data()

            logger.info("Conversion completed successfully")

        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            raise
        finally:
            self._cleanup()

    def _extract_siard(self):
        """Extract SIARD ZIP file to temporary directory."""
        self.temp_dir = Path(tempfile.mkdtemp())
        logger.info(f"Extracting SIARD to {self.temp_dir}")

        with zipfile.ZipFile(self.siard_path, 'r') as zip_file:
            zip_file.extractall(self.temp_dir)

    def _parse_metadata(self):
        """Parse metadata.xml to understand database structure."""
        metadata_path = self.temp_dir / "header" / "metadata.xml"
        if not metadata_path.exists():
            raise FileNotFoundError("metadata.xml not found in SIARD archive")

        logger.info("Parsing metadata.xml")

        # Parse XML with namespace handling
        tree = etree.parse(str(metadata_path))
        root = tree.getroot()

        # Handle namespaces properly
        nsmap = root.nsmap.copy() if root.nsmap else {}

        # If there's a default namespace, give it a prefix for XPath
        if None in nsmap:
            nsmap['siard'] = nsmap[None]
            del nsmap[None]
            ns_prefix = 'siard:'
        else:
            ns_prefix = ''

        # Find all schemas
        schema_xpath = f'.//{ns_prefix}schema'
        for schema_elem in root.xpath(schema_xpath, namespaces=nsmap):
            schema_info = self._parse_schema(schema_elem, nsmap, ns_prefix)
            if schema_info:
                self.schemas.append(schema_info)

    def _parse_schema(self, schema_elem, nsmap: Dict, ns_prefix: str) -> Optional[Dict]:
        """Parse a single schema element."""
        logger.debug(f"Parsing schema element: {schema_elem.tag}")
        logger.debug(f"Schema element children: {[child.tag for child in schema_elem]}")

        # Debug: show all text content in schema element
        for child in schema_elem:
            if child.text and child.text.strip():
                logger.debug(f"  {child.tag}: {child.text.strip()}")

        schema_name = self._get_element_text(schema_elem, f'{ns_prefix}n', nsmap)
        if not schema_name:
            # Try without namespace prefix as fallback
            schema_name = self._get_element_text(schema_elem, 'n', nsmap)
        if not schema_name:
            logger.error("Could not find schema name")
            logger.error(f"Tried xpath: '{ns_prefix}n' and 'n'")
            # Let's try direct child inspection
            for child in schema_elem:
                logger.error(f"  Child: {child.tag} = {child.text}")
            return None

        logger.info(f"Processing schema: {schema_name}")

        # Get the actual folder name from SIARD metadata
        schema_folder_name = self._get_element_text(schema_elem, f'{ns_prefix}folder', nsmap)
        if not schema_folder_name:
            schema_folder_name = self._get_element_text(schema_elem, 'folder', nsmap)

        schema_info = {
            'name': schema_name,
            'folder': schema_folder_name or schema_name,
            'tables': [],
            'views': []
        }

        # Parse tables
        tables_found = self._find_elements_by_xpath(schema_elem, 'table', nsmap, ns_prefix)

        if not tables_found:
            logger.warning("No tables found in schema")
            return schema_info

        for table_index, table_elem in enumerate(tables_found):
            table_info = self._parse_table(table_elem, nsmap, ns_prefix, table_index + 1)
            if table_info:
                schema_info['tables'].append(table_info)

        # Parse views
        views_found = self._find_elements_by_xpath(schema_elem, 'view', nsmap, ns_prefix)
        for view_elem in views_found:
            view_info = self._parse_view(view_elem, nsmap, ns_prefix)
            if view_info:
                schema_info['views'].append(view_info)

        logger.info(f"Schema {schema_name} contains {len(schema_info['tables'])} tables and {len(schema_info['views'])} views")
        return schema_info

    def _parse_table(self, table_elem, nsmap: Dict, ns_prefix: str, table_index: int = 1) -> Optional[Dict]:
        """Parse a single table element."""
        table_name = self._get_element_text(table_elem, f'{ns_prefix}n', nsmap)
        if not table_name:
            # Try without namespace prefix as fallback
            table_name = self._get_element_text(table_elem, 'n', nsmap)
        if not table_name:
            logger.error("Could not find table name")
            return None

        logger.info(f"Processing table: {table_name}")

        table_info = {
            'name': self._sanitize_name(table_name),
            'columns': [],
            'primary_key': [],
            'foreign_keys': [],
            'table_index': table_index
        }

        # Parse columns
        columns_found = self._find_elements_by_xpath(table_elem, 'column', nsmap, ns_prefix)

        if not columns_found:
            logger.warning(f"No columns found for table {table_name}")
            return None

        for column_elem in columns_found:
            column_info = self._parse_column(column_elem, nsmap, ns_prefix)
            if column_info:
                table_info['columns'].append(column_info)

        if not table_info['columns']:
            logger.warning(f"No valid columns parsed for table {table_name}")
            return None

        logger.info(
            f"Table {table_name} has {len(table_info['columns'])} columns: {[c['name'] for c in table_info['columns']]}")

        # Parse primary key
        pk_elems = self._find_elements_by_xpath(table_elem, 'primaryKey', nsmap, ns_prefix)
        if pk_elems:
            for pk_elem in pk_elems:
                column_elems = self._find_elements_by_xpath(pk_elem, 'column', nsmap, ns_prefix)
                for column_elem in column_elems:
                    pk_column = self._get_element_text(column_elem, '.', nsmap)
                    if pk_column:
                        table_info['primary_key'].append(self._sanitize_name(pk_column))
                if table_info['primary_key']:
                    break

        # Parse foreign keys
        fk_elems = self._find_elements_by_xpath(table_elem, 'foreignKey', nsmap, ns_prefix)
        for fk_elem in fk_elems:
            fk_info = self._parse_foreign_key(fk_elem, nsmap, ns_prefix)
            if fk_info:
                table_info['foreign_keys'].append(fk_info)

        return table_info

    def _parse_view(self, view_elem, nsmap: Dict, ns_prefix: str) -> Optional[Dict]:
        """Parse a single view element."""
        view_name = self._get_element_text(view_elem, f'{ns_prefix}n', nsmap)
        if not view_name:
            view_name = self._get_element_text(view_elem, 'n', nsmap)
        if not view_name:
            logger.error("Could not find view name")
            return None

        logger.info(f"Processing view: {view_name}")

        # Get view query/definition - try both query and queryOriginal
        query = self._get_element_text(view_elem, f'{ns_prefix}query', nsmap)
        if not query:
            query = self._get_element_text(view_elem, 'query', nsmap)
        if not query:
            query = self._get_element_text(view_elem, f'{ns_prefix}queryOriginal', nsmap)
        if not query:
            query = self._get_element_text(view_elem, 'queryOriginal', nsmap)
        
        # Get view description
        description = self._get_element_text(view_elem, f'{ns_prefix}description', nsmap)
        if not description:
            description = self._get_element_text(view_elem, 'description', nsmap)

        view_info = {
            'name': self._sanitize_name(view_name),
            'query': query,
            'description': description,
            'columns': []
        }

        # Parse view columns (similar to table columns)
        columns_found = self._find_elements_by_xpath(view_elem, 'column', nsmap, ns_prefix)
        for column_elem in columns_found:
            column_info = self._parse_column(column_elem, nsmap, ns_prefix)
            if column_info:
                view_info['columns'].append(column_info)

        logger.info(f"View {view_name} has {len(view_info['columns'])} columns")
        return view_info

    def _parse_column(self, column_elem, nsmap: Dict, ns_prefix: str) -> Optional[Dict]:
        """Parse a single column element."""
        column_name = self._get_element_text(column_elem, f'{ns_prefix}n', nsmap)
        if not column_name:
            # Try without namespace prefix as fallback
            column_name = self._get_element_text(column_elem, 'n', nsmap)

        column_type = self._get_element_text(column_elem, f'{ns_prefix}type', nsmap)
        if not column_type:
            column_type = self._get_element_text(column_elem, 'type', nsmap)

        nullable = self._get_element_text(column_elem, f'{ns_prefix}nullable', nsmap)
        if not nullable:
            nullable = self._get_element_text(column_elem, 'nullable', nsmap)

        if not column_name or not column_type:
            # Enhanced debugging for missing column data (common in SIARD files with placeholder elements)
            logger.debug(f"Skipping empty column element: name={column_name}, type={column_type}")
            logger.debug(f"Column element tag: {column_elem.tag}")
            logger.debug(f"Column element children: {[child.tag for child in column_elem]}")
            logger.debug(f"Column element text content:")
            for child in column_elem:
                if child.text and child.text.strip():
                    logger.debug(f"  {child.tag}: '{child.text.strip()}'")
                else:
                    logger.debug(f"  {child.tag}: <empty>")
            return None

        # Map SIARD type to SQLite type
        sqlite_type = self.TYPE_MAPPING.get(column_type.upper(), 'TEXT')

        logger.debug(f"Column: {column_name} ({column_type} -> {sqlite_type})")

        return {
            'name': self._sanitize_name(column_name),
            'type': sqlite_type,
            'nullable': nullable != 'false',
            'original_type': column_type
        }

    def _parse_foreign_key(self, fk_elem, nsmap: Dict, ns_prefix: str) -> Optional[Dict]:
        """Parse foreign key information."""
        fk_name = self._get_element_text(fk_elem, f'{ns_prefix}n', nsmap)
        if not fk_name:
            fk_name = self._get_element_text(fk_elem, 'n', nsmap)
        
        referenced_schema = self._get_element_text(fk_elem, f'{ns_prefix}referencedSchema', nsmap)
        if not referenced_schema:
            referenced_schema = self._get_element_text(fk_elem, 'referencedSchema', nsmap)
            
        referenced_table = self._get_element_text(fk_elem, f'{ns_prefix}referencedTable', nsmap)
        if not referenced_table:
            referenced_table = self._get_element_text(fk_elem, 'referencedTable', nsmap)

        if not referenced_table:
            logger.debug("Foreign key missing referenced table")
            return None

        # Parse column references
        source_columns = []
        target_columns = []
        
        # Look for column references in foreign key
        ref_elems = self._find_elements_by_xpath(fk_elem, 'reference', nsmap, ns_prefix)
        for ref_elem in ref_elems:
            source_col = self._get_element_text(ref_elem, f'{ns_prefix}column', nsmap)
            if not source_col:
                source_col = self._get_element_text(ref_elem, 'column', nsmap)
            
            target_col = self._get_element_text(ref_elem, f'{ns_prefix}referenced', nsmap)
            if not target_col:
                target_col = self._get_element_text(ref_elem, 'referenced', nsmap)
            
            if source_col:
                source_columns.append(self._sanitize_name(source_col))
            if target_col:
                target_columns.append(self._sanitize_name(target_col))

        return {
            'name': self._sanitize_name(fk_name) if fk_name else None,
            'referenced_table': self._sanitize_name(referenced_table),
            'referenced_schema': referenced_schema,
            'source_columns': source_columns,
            'target_columns': target_columns
        }

    def _get_element_text(self, parent_elem, xpath: str, nsmap: Dict) -> Optional[str]:
        """Get text content from XML element with robust fallback strategies."""
        # Try the exact xpath first
        try:
            elements = parent_elem.xpath(xpath, namespaces=nsmap if nsmap else None)
            if elements and elements[0].text:
                return elements[0].text.strip()
        except Exception:
            pass

        # If that fails, try some common alternatives
        alternatives = []

        if xpath.endswith('n'):
            # For SIARD <n> elements, try alternatives
            alternatives = [
                'n',
                '*[local-name()="n"]',
                'name',
                '*[local-name()="name"]'
            ]
        elif xpath.endswith('type'):
            alternatives = [
                'type',
                '*[local-name()="type"]'
            ]
        elif xpath.endswith('nullable'):
            alternatives = [
                'nullable',
                '*[local-name()="nullable"]'
            ]
        elif xpath.endswith('folder'):
            alternatives = [
                'folder',
                '*[local-name()="folder"]'
            ]
        else:
            # Generic fallback
            alternatives = [xpath.split(':')[-1] if ':' in xpath else xpath]

        # Try each alternative
        for alt_xpath in alternatives:
            try:
                elements = parent_elem.xpath(alt_xpath, namespaces=nsmap if nsmap else None)
                if elements and elements[0].text:
                    return elements[0].text.strip()
            except Exception:
                continue

        # Last resort: check direct children
        for child in parent_elem:
            tag_name = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            if tag_name in ['n', 'name'] and xpath.endswith(('n', 'name')):
                if child.text:
                    return child.text.strip()
            elif tag_name == 'type' and xpath.endswith('type'):
                if child.text:
                    return child.text.strip()
            elif tag_name == 'nullable' and xpath.endswith('nullable'):
                if child.text:
                    return child.text.strip()

        return None

    def _find_elements_by_xpath(self, parent_elem, element_name: str, nsmap: Dict, ns_prefix: str) -> List:
        """Find elements using multiple XPath strategies with fallbacks."""
        xpath_options = [
            f'.//{ns_prefix}{element_name}',
            f'.//{element_name}',
            f'.//*[local-name()="{element_name}"]'
        ]
        
        for xpath in xpath_options:
            try:
                elements = parent_elem.xpath(xpath, namespaces=nsmap if nsmap else None)
                if elements:
                    logger.debug(f"Found {len(elements)} {element_name} elements using xpath: {xpath}")
                    return elements
            except Exception as e:
                logger.debug(f"XPath {xpath} failed: {e}")
        
        logger.warning(f"No {element_name} elements found")
        return []

    def _sanitize_name(self, name: str) -> str:
        """Sanitize table/column names for SQLite."""
        # Remove problematic characters, keep it simple
        sanitized = re.sub(r'[^\w]', '_', name)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        return sanitized or "unnamed"

    @contextmanager
    def _sqlite_connection(self):
        """Context manager for SQLite database connections."""
        conn = sqlite3.connect(self.sqlite_path)
        try:
            yield conn
        finally:
            conn.close()

    def _create_sqlite_database(self):
        """Create SQLite database with schema."""
        logger.info(f"Creating SQLite database: {self.sqlite_path}")

        if not self.schemas:
            logger.error("No schemas found - cannot create database!")
            return

        logger.info(f"Found {len(self.schemas)} schemas to process")
        for schema in self.schemas:
            logger.info(f"  Schema '{schema['name']}' has {len(schema['tables'])} tables")
            for table in schema['tables']:
                logger.info(f"    Table '{table['name']}' has {len(table['columns'])} columns")

        # Remove existing database
        if self.sqlite_path.exists():
            self.sqlite_path.unlink()

        with self._sqlite_connection() as conn:
            cursor = conn.cursor()
            
            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")

            # Create tables first (views may depend on them)
            tables_created = 0
            for schema in self.schemas:
                for table in schema['tables']:
                    try:
                        self._create_table(cursor, table)
                        tables_created += 1
                    except Exception as e:
                        logger.error(f"Failed to create table {table['name']}: {e}")

            # Create views after tables
            views_created = 0
            for schema in self.schemas:
                for view in schema['views']:
                    try:
                        self._create_view(cursor, view)
                        views_created += 1
                    except Exception as e:
                        logger.error(f"Failed to create view {view['name']}: {e}")

            conn.commit()
            logger.info(f"Successfully created {tables_created} tables and {views_created} views")

    def _create_table(self, cursor, table_info: Dict):
        """Create a single table in SQLite."""
        table_name = table_info['name']
        columns = table_info['columns']
        primary_key = table_info['primary_key']
        foreign_keys = table_info['foreign_keys']

        logger.info(f"Creating table: {table_name}")

        # Build column definitions
        column_defs = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            # Be more careful about NOT NULL constraints
            # Only add NOT NULL if explicitly required and it's not a problematic type
            if not col['nullable'] and col['original_type'].upper() != 'BOOLEAN':
                col_def += " NOT NULL"
            column_defs.append(col_def)

        # Add primary key constraint
        if primary_key:
            pk_cols = ", ".join(primary_key)
            column_defs.append(f"PRIMARY KEY ({pk_cols})")

        # Add foreign key constraints
        for fk in foreign_keys:
            if fk['source_columns'] and fk['target_columns']:
                source_cols = ", ".join(fk['source_columns'])
                target_cols = ", ".join(fk['target_columns'])
                fk_def = f"FOREIGN KEY ({source_cols}) REFERENCES {fk['referenced_table']} ({target_cols})"
                column_defs.append(fk_def)
                logger.debug(f"Added foreign key: {fk_def}")

        # Create table
        create_sql = f"CREATE TABLE {table_name} (\n  {',\n  '.join(column_defs)}\n)"
        logger.debug(f"SQL: {create_sql}")
        cursor.execute(create_sql)

    def _create_view(self, cursor, view_info: Dict):
        """Create a single view in SQLite."""
        view_name = view_info['name']
        query = view_info['query']

        logger.info(f"Creating view: {view_name}")

        if not query:
            logger.warning(f"View {view_name} has no query definition, skipping")
            return

        # Basic SQL query cleanup for SQLite compatibility
        sqlite_query = self._convert_query_to_sqlite(query)
        
        create_sql = f"CREATE VIEW {view_name} AS {sqlite_query}"
        logger.debug(f"SQL: {create_sql}")
        
        try:
            cursor.execute(create_sql)
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            logger.error(f"Original query: {query}")
            logger.error(f"Converted query: {sqlite_query}")

    def _convert_query_to_sqlite(self, query: str) -> str:
        """Convert SIARD SQL query to SQLite-compatible format."""
        if not query:
            return query
            
        # Basic cleanup - remove common SQL differences
        sqlite_query = query.strip()
        
        import re
        
        # Handle MySQL CREATE VIEW syntax - extract just the SELECT part
        if sqlite_query.upper().startswith('CREATE'):
            # Extract the SELECT statement from CREATE VIEW syntax
            # Pattern: CREATE ... VIEW ... AS select_statement
            view_match = re.search(r'VIEW\s+.*?\s+AS\s+(.+)', sqlite_query, re.IGNORECASE | re.DOTALL)
            if view_match:
                sqlite_query = view_match.group(1).strip()
        
        # Remove MySQL-specific syntax elements
        sqlite_query = re.sub(r'\bALGORITHM=\w+\s+', '', sqlite_query, flags=re.IGNORECASE)
        sqlite_query = re.sub(r'\bDEFINER=`[^`]+`@`[^`]+`\s+', '', sqlite_query, flags=re.IGNORECASE)
        sqlite_query = re.sub(r'\bSQL\s+SECURITY\s+\w+\s+', '', sqlite_query, flags=re.IGNORECASE)
        
        # Remove backticks (MySQL table/column quotes) - SQLite uses double quotes if needed
        sqlite_query = re.sub(r'`([^`]+)`', r'\1', sqlite_query)
        
        # Replace AS aliases that might conflict
        sqlite_query = re.sub(r'\s+AS\s+(\w+)', r' AS \1', sqlite_query, flags=re.IGNORECASE)
        
        # Replace common SQL Server/Oracle syntax with SQLite equivalents
        sqlite_query = re.sub(r'\bTOP\s+\d+\b', '', sqlite_query, flags=re.IGNORECASE)
        
        # Fix the LIMIT/OFFSET regex - it had incorrect group references
        # This pattern looks for LIMIT num OFFSET num and swaps them
        sqlite_query = re.sub(r'\bLIMIT\s+(\d+)\s+OFFSET\s+(\d+)\b', r'LIMIT \2 OFFSET \1', sqlite_query, flags=re.IGNORECASE)
        
        # Convert boolean literals to SQLite format
        sqlite_query = re.sub(r'\btrue\b', '1', sqlite_query, flags=re.IGNORECASE)
        sqlite_query = re.sub(r'\bfalse\b', '0', sqlite_query, flags=re.IGNORECASE)
        
        # Remove trailing semicolons
        sqlite_query = sqlite_query.rstrip(';')
        
        return sqlite_query

    def _import_data(self):
        """Import data from SIARD XML files."""
        logger.info("Importing data")

        with self._sqlite_connection() as conn:
            cursor = conn.cursor()
            
            for schema_index, schema in enumerate(self.schemas):
                # Try the folder name specified in metadata first, then fallbacks
                possible_schema_folders = [
                    self.temp_dir / "content" / schema['folder'],
                    self.temp_dir / "content" / f"table{schema_index + 1}",
                    self.temp_dir / "content" / schema['name']
                ]
                
                schema_folder = None
                for folder in possible_schema_folders:
                    if folder.exists():
                        schema_folder = folder
                        logger.info(f"Using schema folder: {schema_folder}")
                        break
                
                if not schema_folder:
                    logger.warning(f"No schema folder found for {schema['name']}. Tried: {[str(f) for f in possible_schema_folders]}")
                    continue

                for table in schema['tables']:
                    self._import_table_data(cursor, table, schema_folder)

            conn.commit()

    def _import_table_data(self, cursor, table_info: Dict, schema_folder: Path):
        """Import data for a single table."""
        table_name = table_info['name']
        # Try both possible file naming conventions - generic names first (most common)
        table_index = table_info.get('table_index', 1)
        possible_files = [
            schema_folder / f"table{table_index}" / f"table{table_index}.xml",
            schema_folder / f"table{table_index}.xml",
            schema_folder / f"{table_name}.xml",
            schema_folder / f"{table_name}" / f"{table_name}.xml"
        ]

        table_file = None
        for file_path in possible_files:
            if file_path.exists():
                table_file = file_path
                break

        if not table_file:
            logger.warning(f"Data file not found for table {table_name}. Tried: {[str(f) for f in possible_files]}")
            return

        logger.info(f"Importing data for table: {table_name} from {table_file}")

        # Parse table data XML - use streaming for large files
        try:
            # Check file size first
            file_size = table_file.stat().st_size
            use_streaming = file_size > self.STREAMING_THRESHOLD_MB * 1024 * 1024
            
            if use_streaming:
                logger.info(f"Large file detected ({file_size / 1024 / 1024:.1f}MB), using streaming parser")
                self._import_table_data_streaming(cursor, table_info, table_file, table_name)
                return
            
            tree = etree.parse(str(table_file))
            root = tree.getroot()

            logger.debug(f"Root element: {root.tag}")
            logger.debug(f"Namespaces: {root.nsmap}")

            # Handle namespaces in data XML - use local-name() to ignore namespaces
            # This is more robust than trying to manage namespace prefixes

            # Get column names for INSERT
            column_names = [col['name'] for col in table_info['columns']]
            placeholders = ', '.join(['?' for _ in column_names])
            insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"

            logger.debug(f"SQL: {insert_sql}")

            # Use local-name() to find rows regardless of namespace
            rows = root.xpath('.//*[local-name()="row"]')
            logger.debug(f"Found {len(rows)} rows using local-name xpath")

            # Process each row
            rows_imported = 0
            for row_elem in rows:
                row_data = []

                # Process each column in order
                for i, col_info in enumerate(table_info['columns']):
                    # SIARD uses c1, c2, c3, etc. for column values
                    # Use local-name() to ignore namespace
                    col_elements = row_elem.xpath(f'*[local-name()="c{i + 1}"]')

                    value = None
                    if col_elements:
                        col_elem = col_elements[0]
                        # Check for xsi:nil attribute (NULL values)
                        nil_attr = col_elem.get('{http://www.w3.org/2001/XMLSchema-instance}nil')
                        if nil_attr == 'true':
                            value = None
                        elif col_elem.text is not None:
                            value = col_elem.text.strip()
                            # Convert based on type
                            if col_info['type'] == 'INTEGER':
                                if col_info['original_type'].upper() == 'BOOLEAN':
                                    # Handle boolean conversion first
                                    if value.lower() in ('true', '1'):
                                        value = 1
                                    elif value.lower() in ('false', '0'):
                                        value = 0
                                    else:
                                        logger.warning(f"Unknown boolean value: '{value}', defaulting to 0")
                                        value = 0
                                elif value:
                                    # Regular integer conversion
                                    try:
                                        value = int(value)
                                    except ValueError:
                                        logger.warning(f"Could not convert '{value}' to INTEGER")
                                        value = None
                            elif col_info['type'] == 'REAL' and value:
                                try:
                                    value = float(value)
                                except ValueError:
                                    logger.warning(f"Could not convert '{value}' to REAL")
                                    value = None

                    row_data.append(value)

                # Insert the row
                try:
                    cursor.execute(insert_sql, row_data)
                    rows_imported += 1

                    if rows_imported <= 3:  # Log first few rows for debugging
                        logger.debug(f"Row {rows_imported}: {row_data}")

                except Exception as e:
                    logger.error(f"Error inserting row {rows_imported + 1}: {e}")
                    logger.error(f"Row data: {row_data}")

                if rows_imported % self.PROGRESS_INTERVAL == 0 and rows_imported > 0:
                    logger.debug(f"Imported {rows_imported} rows for {table_name}")

            logger.info(f"Completed import for {table_name}: {rows_imported} rows")

        except Exception as e:
            logger.error(f"Error importing data for {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _import_table_data_streaming(self, cursor, table_info: Dict, table_file: Path, table_name: str):
        """Import data from large XML files using streaming parser."""
        from lxml import etree
        
        # Get column names for INSERT
        column_names = [col['name'] for col in table_info['columns']]
        placeholders = ', '.join(['?' for _ in column_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
        
        logger.debug(f"SQL: {insert_sql}")
        
        rows_imported = 0
        batch_data = []
        
        try:
            # Use iterparse for streaming
            context = etree.iterparse(str(table_file), events=('start', 'end'))
            context = iter(context)
            
            # Skip root element
            event, root = next(context)
            
            for event, elem in context:
                if event == 'end' and elem.tag.endswith('}row'):
                    # Process row element
                    row_data = []
                    
                    # Process each column in order
                    for i, col_info in enumerate(table_info['columns']):
                        # Find column element (c1, c2, etc.)
                        col_elem = None
                        for child in elem:
                            if child.tag.endswith(f'}}c{i + 1}'):
                                col_elem = child
                                break
                        
                        value = None
                        if col_elem is not None:
                            # Check for xsi:nil attribute (NULL values)
                            nil_attr = col_elem.get('{http://www.w3.org/2001/XMLSchema-instance}nil')
                            if nil_attr == 'true':
                                value = None
                            elif col_elem.text is not None:
                                value = col_elem.text.strip()
                                # Convert based on type
                                if col_info['type'] == 'INTEGER':
                                    if col_info['original_type'].upper() == 'BOOLEAN':
                                        # Handle boolean conversion first
                                        if value.lower() in ('true', '1'):
                                            value = 1
                                        elif value.lower() in ('false', '0'):
                                            value = 0
                                        else:
                                            logger.warning(f"Unknown boolean value: '{value}', defaulting to 0")
                                            value = 0
                                    elif value:
                                        # Regular integer conversion
                                        try:
                                            value = int(value)
                                        except ValueError:
                                            logger.warning(f"Could not convert '{value}' to INTEGER")
                                            value = None
                                elif col_info['type'] == 'REAL' and value:
                                    try:
                                        value = float(value)
                                    except ValueError:
                                        logger.warning(f"Could not convert '{value}' to REAL")
                                        value = None
                        
                        row_data.append(value)
                    
                    batch_data.append(row_data)
                    
                    # Insert batch when full
                    if len(batch_data) >= self.BATCH_SIZE:
                        try:
                            cursor.executemany(insert_sql, batch_data)
                            rows_imported += len(batch_data)
                            batch_data = []
                            
                            if rows_imported % self.PROGRESS_INTERVAL == 0:
                                logger.debug(f"Imported {rows_imported} rows for {table_name}")
                        except Exception as e:
                            logger.error(f"Error inserting batch at row {rows_imported}: {e}")
                            break
                    
                    # Clean up element to free memory
                    elem.clear()
                    # Also clean up previous siblings to save memory
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
            
            # Insert remaining batch
            if batch_data:
                try:
                    cursor.executemany(insert_sql, batch_data)
                    rows_imported += len(batch_data)
                except Exception as e:
                    logger.error(f"Error inserting final batch: {e}")
            
            logger.info(f"Completed import for {table_name}: {rows_imported} rows")
            
        except Exception as e:
            logger.error(f"Error in streaming import for {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _cleanup(self):
        """Clean up temporary files."""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)


def main():
    """Command line interface."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description='Convert SIARD files to SQLite databases',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  siard-convert employees.siard employees.db
  siard-convert --verbose data.siard output.sqlite
  siard2sqlite --quiet archive.siard result.db

For more information, visit: https://github.com/your-repo/siard-sqlite
        """
    )
    
    parser.add_argument('siard_file', help='Path to SIARD file')
    parser.add_argument('sqlite_file', help='Output SQLite file path')
    
    # Logging options
    log_group = parser.add_mutually_exclusive_group()
    log_group.add_argument('-v', '--verbose', action='store_true', 
                          help='Enable verbose logging (shows detailed progress)')
    log_group.add_argument('-q', '--quiet', action='store_true',
                          help='Suppress all output except errors')
    
    # Feature options
    parser.add_argument('--no-foreign-keys', action='store_true',
                       help='Skip creating foreign key constraints')
    parser.add_argument('--no-views', action='store_true',
                       help='Skip creating views')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for data import (default: 1000)')
    parser.add_argument('--streaming-threshold', type=int, default=50,
                       help='File size threshold in MB for streaming parser (default: 50)')
    
    # Information options
    parser.add_argument('--version', action='version', version='siard-sqlite 0.1.0')

    args = parser.parse_args()

    # Configure logging
    if args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    try:
        converter = SiardToSqlite(args.siard_file, args.sqlite_file)
        
        # Apply command line options
        if hasattr(converter, 'BATCH_SIZE'):
            converter.BATCH_SIZE = args.batch_size
        if hasattr(converter, 'STREAMING_THRESHOLD_MB'):
            converter.STREAMING_THRESHOLD_MB = args.streaming_threshold
            
        converter.convert()

        if not args.quiet:
            print(f"✅ Conversion completed: {args.sqlite_file}")
            print(f"📊 You can explore the data with: datasette {args.sqlite_file}")
            
    except FileNotFoundError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if args.verbose:
            import traceback
            traceback.print_exc()
        else:
            print(f"❌ Conversion failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()