"""SIARD to SQLite Converter

A Python tool for converting SIARD (Software Independent Archival of Relational Databases) 
archive files to SQLite databases for analysis and exploration.
"""

from .converter import SiardToSqlite, main

__version__ = "0.1.0"
__all__ = ["SiardToSqlite", "main"]