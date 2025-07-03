#!/usr/bin/env python3
"""
Skapar en enkel test-SIARD-fil för att testa konverteraren.
"""

import zipfile
import os
from pathlib import Path
from datetime import datetime

def create_test_siard(output_path="test_database.siard"):
    """Skapar en enkel test-SIARD-fil med två relaterade tabeller."""
    
    # Skapa temporär mapp för SIARD-innehåll
    temp_dir = Path("temp_siard")
    temp_dir.mkdir(exist_ok=True)
    
    try:
        # Skapa mappstruktur
        header_dir = temp_dir / "header"
        content_dir = temp_dir / "content" / "schema1"
        header_dir.mkdir(parents=True, exist_ok=True)
        content_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Skapa metadata.xml
        metadata_xml = create_metadata_xml()
        with open(header_dir / "metadata.xml", "w", encoding="utf-8") as f:
            f.write(metadata_xml)
        
        # 2. Skapa table0.xsd (schema för customers)
        customers_xsd = create_customers_schema()
        with open(header_dir / "table0.xsd", "w", encoding="utf-8") as f:
            f.write(customers_xsd)
        
        # 3. Skapa table1.xsd (schema för orders)
        orders_xsd = create_orders_schema()
        with open(header_dir / "table1.xsd", "w", encoding="utf-8") as f:
            f.write(orders_xsd)
        
        # 4. Skapa customers.xml (data)
        customers_data = create_customers_data()
        with open(content_dir / "customers.xml", "w", encoding="utf-8") as f:
            f.write(customers_data)
        
        # 5. Skapa orders.xml (data)
        orders_data = create_orders_data()
        with open(content_dir / "orders.xml", "w", encoding="utf-8") as f:
            f.write(orders_data)
        
        # 6. Packa ihop till ZIP-fil
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    archive_name = file_path.relative_to(temp_dir)
                    zf.write(file_path, archive_name)
        
        print(f"Test-SIARD-fil skapad: {output_path}")
        return True
        
    finally:
        # Städa upp temporära filer
        import shutil
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

def create_metadata_xml():
    """Skapar metadata.xml enligt SIARD-specifikationen."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" 
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
              xsi:schemaLocation="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd metadata.xsd"
              version="2.1">
  <dbname>test_database</dbname>
  <description>Test database för SIARD-konvertering</description>
  <archiver>Test Creator</archiver>
  <archiverContact>test@example.com</archiverContact>
  <dataOwner>Test Organization</dataOwner>
  <dataOriginTimespan>2024-01-01 - {current_date}</dataOriginTimespan>
  <producerApplication>Test Script v1.0</producerApplication>
  <archivalDate>{current_date}</archivalDate>
  <messageDigest algorithm="MD5">placeholder_digest</messageDigest>
  <clientMachine>test-machine</clientMachine>
  <databaseProduct>Generic Database</databaseProduct>
  <connection>test://localhost/testdb</connection>
  <databaseUser>testuser</databaseUser>
  
  <schemas>
    <schema>
      <name>schema1</name>
      <folder>schema1</folder>
      <description>Test schema med kunder och beställningar</description>
      
      <tables>
        <!-- Customers table -->
        <table>
          <name>customers</name>
          <folder>customers.xml</folder>
          <description>Kundtabell</description>
          <columns>
            <column>
              <name>customer_id</name>
              <type>INTEGER</type>
              <nullable>false</nullable>
              <description>Unikt kund-ID</description>
            </column>
            <column>
              <name>name</name>
              <type>VARCHAR</type>
              <typeLength>100</typeLength>
              <nullable>false</nullable>
              <description>Kundnamn</description>
            </column>
            <column>
              <name>email</name>
              <type>VARCHAR</type>
              <typeLength>255</typeLength>
              <nullable>true</nullable>
              <description>E-postadress</description>
            </column>
            <column>
              <name>created_date</name>
              <type>DATE</type>
              <nullable>false</nullable>
              <description>Datum när kunden skapades</description>
            </column>
            <column>
              <name>is_active</name>
              <type>BOOLEAN</type>
              <nullable>false</nullable>
              <description>Om kunden är aktiv</description>
            </column>
          </columns>
          
          <primaryKey>
            <name>pk_customers</name>
            <column>customer_id</column>
          </primaryKey>
          
          <rows>5</rows>
        </table>
        
        <!-- Orders table -->
        <table>
          <name>orders</name>
          <folder>orders.xml</folder>
          <description>Beställningstabell</description>
          <columns>
            <column>
              <name>order_id</name>
              <type>INTEGER</type>
              <nullable>false</nullable>
              <description>Unikt beställnings-ID</description>
            </column>
            <column>
              <name>customer_id</name>
              <type>INTEGER</type>
              <nullable>false</nullable>
              <description>Referens till kund</description>
            </column>
            <column>
              <name>order_date</name>
              <type>TIMESTAMP</type>
              <nullable>false</nullable>
              <description>Beställningsdatum</description>
            </column>
            <column>
              <name>amount</name>
              <type>DECIMAL</type>
              <typePrecision>10</typePrecision>
              <typeScale>2</typeScale>
              <nullable>false</nullable>
              <description>Beställningsbelopp</description>
            </column>
            <column>
              <name>status</name>
              <type>VARCHAR</type>
              <typeLength>50</typeLength>
              <nullable>false</nullable>
              <description>Beställningsstatus</description>
            </column>
          </columns>
          
          <primaryKey>
            <name>pk_orders</name>
            <column>order_id</column>
          </primaryKey>
          
          <foreignKeys>
            <foreignKey>
              <name>fk_orders_customer</name>
              <column>customer_id</column>
              <referencedSchema>schema1</referencedSchema>
              <referencedTable>customers</referencedTable>
              <referencedColumn>customer_id</referencedColumn>
            </foreignKey>
          </foreignKeys>
          
          <rows>8</rows>
        </table>
      </tables>
      
      <views>
        <!-- View för aktiva kunder -->
        <view>
          <name>active_customers</name>
          <query>SELECT customer_id, name, email, created_date FROM customers WHERE is_active = true</query>
          <description>Visa endast aktiva kunder</description>
          <columns>
            <column>
              <name>customer_id</name>
              <type>INTEGER</type>
              <nullable>false</nullable>
              <description>Unikt kund-ID</description>
            </column>
            <column>
              <name>name</name>
              <type>VARCHAR</type>
              <typeLength>100</typeLength>
              <nullable>false</nullable>
              <description>Kundnamn</description>
            </column>
            <column>
              <name>email</name>
              <type>VARCHAR</type>
              <typeLength>255</typeLength>
              <nullable>true</nullable>
              <description>E-postadress</description>
            </column>
            <column>
              <name>created_date</name>
              <type>DATE</type>
              <nullable>false</nullable>
              <description>Datum när kunden skapades</description>
            </column>
          </columns>
        </view>
        
        <!-- View för order-sammanfattning -->
        <view>
          <name>order_summary</name>
          <query>SELECT c.name, c.email, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.name, c.email</query>
          <description>Sammanfattning av beställningar per kund</description>
          <columns>
            <column>
              <name>name</name>
              <type>VARCHAR</type>
              <typeLength>100</typeLength>
              <nullable>false</nullable>
              <description>Kundnamn</description>
            </column>
            <column>
              <name>email</name>
              <type>VARCHAR</type>
              <typeLength>255</typeLength>
              <nullable>true</nullable>
              <description>E-postadress</description>
            </column>
            <column>
              <name>order_count</name>
              <type>INTEGER</type>
              <nullable>false</nullable>
              <description>Antal beställningar</description>
            </column>
            <column>
              <name>total_amount</name>
              <type>DECIMAL</type>
              <typePrecision>10</typePrecision>
              <typeScale>2</typeScale>
              <nullable>true</nullable>
              <description>Totalt belopp</description>
            </column>
          </columns>
        </view>
      </views>
    </schema>
  </schemas>
</siardArchive>'''

def create_customers_schema():
    """Skapar XSD-schema för customers-tabellen."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" 
           targetNamespace="http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
           xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
           elementFormDefault="qualified" 
           attributeFormDefault="unqualified">
  
  <xs:element name="table">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="row" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="c1" type="xs:int"/>
              <xs:element name="c2" type="xs:string"/>
              <xs:element name="c3" type="xs:string" nillable="true"/>
              <xs:element name="c4" type="xs:date"/>
              <xs:element name="c5" type="xs:boolean"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

def create_orders_schema():
    """Skapar XSD-schema för orders-tabellen."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" 
           targetNamespace="http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
           xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
           elementFormDefault="qualified" 
           attributeFormDefault="unqualified">
  
  <xs:element name="table">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="row" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="c1" type="xs:int"/>
              <xs:element name="c2" type="xs:int"/>
              <xs:element name="c3" type="xs:dateTime"/>
              <xs:element name="c4" type="xs:decimal"/>
              <xs:element name="c5" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>'''

def create_customers_data():
    """Skapar testdata för customers-tabellen."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">
  <row>
    <c1>1</c1>
    <c2>Anna Andersson</c2>
    <c3>anna@example.com</c3>
    <c4>2023-01-15</c4>
    <c5>true</c5>
  </row>
  <row>
    <c1>2</c1>
    <c2>Bert Bertsson</c2>
    <c3>bert@example.com</c3>
    <c4>2023-02-10</c4>
    <c5>true</c5>
  </row>
  <row>
    <c1>3</c1>
    <c2>Cecilia Carlsson</c2>
    <c3 xsi:nil="true" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
    <c4>2023-03-05</c4>
    <c5>false</c5>
  </row>
  <row>
    <c1>4</c1>
    <c2>David Davidsson</c2>
    <c3>david@example.com</c3>
    <c4>2023-04-20</c4>
    <c5>true</c5>
  </row>
  <row>
    <c1>5</c1>
    <c2>Eva Eriksson</c2>
    <c3>eva@example.com</c3>
    <c4>2023-05-12</c4>
    <c5>true</c5>
  </row>
</table>'''

def create_orders_data():
    """Skapar testdata för orders-tabellen."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">
  <row>
    <c1>101</c1>
    <c2>1</c2>
    <c3>2023-06-01T10:30:00</c3>
    <c4>299.99</c4>
    <c5>completed</c5>
  </row>
  <row>
    <c1>102</c1>
    <c2>1</c2>
    <c3>2023-06-15T14:22:00</c3>
    <c4>149.50</c4>
    <c5>completed</c5>
  </row>
  <row>
    <c1>103</c1>
    <c2>2</c2>
    <c3>2023-06-20T09:15:00</c3>
    <c4>89.99</c4>
    <c5>pending</c5>
  </row>
  <row>
    <c1>104</c1>
    <c2>2</c2>
    <c3>2023-07-01T16:45:00</c3>
    <c4>199.95</c4>
    <c5>completed</c5>
  </row>
  <row>
    <c1>105</c1>
    <c2>4</c2>
    <c3>2023-07-10T11:20:00</c3>
    <c4>59.99</c4>
    <c5>cancelled</c5>
  </row>
  <row>
    <c1>106</c1>
    <c2>4</c2>
    <c3>2023-07-15T13:30:00</c3>
    <c4>399.00</c4>
    <c5>completed</c5>
  </row>
  <row>
    <c1>107</c1>
    <c2>5</c2>
    <c3>2023-07-20T08:45:00</c3>
    <c4>129.99</c4>
    <c5>processing</c5>
  </row>
  <row>
    <c1>108</c1>
    <c2>5</c2>
    <c3>2023-07-25T15:10:00</c3>
    <c4>79.50</c4>
    <c5>completed</c5>
  </row>
</table>'''

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Skapa en test-SIARD-fil')
    parser.add_argument('-o', '--output', default='test_database.siard', 
                       help='Namn på utdata-SIARD-fil (standard: test_database.siard)')
    
    args = parser.parse_args()
    
    success = create_test_siard(args.output)
    if success:
        print(f"\nTest-SIARD-filen '{args.output}' har skapats framgångsrikt!")
        print("\nInnehåller:")
        print("- Schema 'schema1' med två tabeller och två views:")
        print("  * customers (5 rader: kund-ID, namn, e-post, datum, aktiv)")
        print("  * orders (8 rader: order-ID, kund-ID, datum, belopp, status)")
        print("  * active_customers (view: visar endast aktiva kunder)")
        print("  * order_summary (view: sammanfattning av beställningar per kund)")
        print("- Foreign key-relation mellan orders.customer_id -> customers.customer_id")
        print(f"\nDu kan nu testa din konverterare med: python siard_converter.py {args.output} test.sqlite")
    else:
        print("Något gick fel vid skapandet av test-SIARD-filen.")
