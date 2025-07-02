#!/usr/bin/env python3
"""
Debug version av SIARD-konverteraren för att hitta problemet.
"""

import zipfile
import sqlite3
import tempfile
import shutil
from pathlib import Path
from lxml import etree
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def debug_siard_file(siard_path):
    """Undersök SIARD-filens innehåll."""
    print(f"\n=== DEBUGGING SIARD FILE: {siard_path} ===")
    
    if not Path(siard_path).exists():
        print(f"ERROR: File {siard_path} does not exist!")
        return
    
    # 1. Lista innehållet i ZIP-filen
    print("\n1. ZIP File Contents:")
    with zipfile.ZipFile(siard_path, 'r') as zf:
        for info in zf.infolist():
            print(f"   {info.filename} ({info.file_size} bytes)")
    
    # 2. Extrahera och undersök
    temp_dir = Path(tempfile.mkdtemp())
    print(f"\n2. Extracting to: {temp_dir}")
    
    try:
        with zipfile.ZipFile(siard_path, 'r') as zf:
            zf.extractall(temp_dir)
        
        # 3. Kolla metadata.xml
        metadata_path = temp_dir / "header" / "metadata.xml"
        if metadata_path.exists():
            print(f"\n3. Metadata.xml exists: {metadata_path}")
            print("First 1000 chars:")
            print(metadata_path.read_text(encoding='utf-8')[:1000])
            
            # Parsa metadata
            tree = etree.parse(str(metadata_path))
            root = tree.getroot()
            print(f"\nRoot element: {root.tag}")
            print(f"Namespaces: {root.nsmap}")
            
            # Hitta scheman
            schemas = root.xpath('.//schema | .//*[local-name()="schema"]')
            print(f"Found {len(schemas)} schemas")
            
            for i, schema in enumerate(schemas):
                schema_name = None
                for child in schema:
                    if child.tag.endswith('n') or 'name' in child.tag:
                        schema_name = child.text
                        break
                print(f"  Schema {i}: {schema_name}")
                
                # Hitta tabeller
                tables = schema.xpath('.//table | .//*[local-name()="table"]')
                print(f"    Tables: {len(tables)}")
                
                for j, table in enumerate(tables):
                    table_name = None
                    for child in table:
                        if child.tag.endswith('n') or 'name' in child.tag:
                            table_name = child.text
                            break
                    print(f"      Table {j}: {table_name}")
        
        # 4. Kolla content-mappen
        content_dir = temp_dir / "content"
        if content_dir.exists():
            print(f"\n4. Content directory exists: {content_dir}")
            for item in content_dir.rglob("*"):
                if item.is_file():
                    print(f"   {item.relative_to(temp_dir)} ({item.stat().st_size} bytes)")
                    
                    # Om det är en .xml-fil, visa början
                    if item.suffix == '.xml':
                        try:
                            content = item.read_text(encoding='utf-8')
                            print(f"      First 200 chars: {content[:200]}")
                            
                            # Parsa XML
                            tree = etree.parse(str(item))
                            root = tree.getroot()
                            print(f"      Root: {root.tag}, Namespace: {root.nsmap}")
                            
                            # Räkna rader
                            rows = root.xpath('.//row | .//*[local-name()="row"]')
                            print(f"      Rows found: {len(rows)}")
                            
                            if rows:
                                # Visa första raden
                                first_row = rows[0]
                                print(f"      First row children: {[child.tag for child in first_row]}")
                                for child in first_row:
                                    print(f"        {child.tag}: {child.text}")
                                    
                        except Exception as e:
                            print(f"      Error parsing XML: {e}")
        else:
            print("\n4. No content directory found!")
            
    finally:
        shutil.rmtree(temp_dir)

def test_simple_conversion():
    """Testa en förenklad konvertering med debug."""
    print("\n=== TESTING SIMPLE CONVERSION ===")
    
    # Skapa en minimal SIARD-fil för test
    temp_dir = Path("debug_siard_temp")
    temp_dir.mkdir(exist_ok=True)
    
    try:
        # Skapa mappstruktur
        header_dir = temp_dir / "header"
        content_dir = temp_dir / "content" / "schema1"
        header_dir.mkdir(parents=True, exist_ok=True)
        content_dir.mkdir(parents=True, exist_ok=True)
        
        # Enkel metadata.xml
        metadata_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <dbname>debug_db</dbname>
  <schemas>
    <schema>
      <n>schema1</n>
      <tables>
        <table>
          <n>test_table</n>
          <columns>
            <column>
              <n>id</n>
              <type>INTEGER</type>
              <nullable>false</nullable>
            </column>
            <column>
              <n>name</n>
              <type>VARCHAR</type>
              <nullable>true</nullable>
            </column>
          </columns>
          <rows>2</rows>
        </table>
      </tables>
    </schema>
  </schemas>
</siardArchive>'''
        
        with open(header_dir / "metadata.xml", "w", encoding="utf-8") as f:
            f.write(metadata_xml)
        
        # Enkel data-fil
        data_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">
  <row>
    <c1>1</c1>
    <c2>Alice</c2>
  </row>
  <row>
    <c1>2</c1>
    <c2>Bob</c2>
  </row>
</table>'''
        
        with open(content_dir / "test_table.xml", "w", encoding="utf-8") as f:
            f.write(data_xml)
        
        # Packa till ZIP
        debug_siard_path = "debug_test.siard"
        with zipfile.ZipFile(debug_siard_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in temp_dir.rglob("*"):
                if root.is_file():
                    archive_name = root.relative_to(temp_dir)
                    zf.write(root, archive_name)
        
        print(f"Created debug SIARD: {debug_siard_path}")
        
        # Testa konvertering
        debug_siard_file(debug_siard_path)
        
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        debug_siard_file(sys.argv[1])
    else:
        print("Usage: python debug_siard.py <siard_file>")
        print("Or run without arguments to test simple conversion")
        test_simple_conversion()
