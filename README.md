dbdpp
=====

## What is dbdpp?

This is a simple tool to compute differences between two MySQL database tables
as a list of SQL commands (INSERT/UPDATE/DELETE) needed to synchronize them.
It serves as a faster and simpler replacement for [DBDiff](https://github.com/DBDiff/DBDiff).

## How to use?

Running the program with no arguments will print its syntax:
```
USAGE: dbdpp [ source.cfg ] target.cfg source_table_name target_table_name
	(source.cfg and target.cfg should be MySQL-style configuration files)
```

### Example

For example, if **source.cfg** is
```
[client]
host=source-database.example.com
user=abc
password=def
database=db_reference
```
and **target.cfg** is
```
[client]
host=target-database.example.com
port=33060
user=abc
password=def
database=db_to_change
```
running the program as
```
dbdpp source.cfg target.cfg ref_table target_table > out.sql
```
will print into **out.sql** list of SQL statements which should be applied to
_db_to_change.target_table_ to make it consistent with _db_reference.ref_table_.

There are two modes of operation:
* if both **source.cfg** and **target.cfg** are given (even if they are the same),
  the tool will fetch all data from both tables and perform comparisons on your local machine;
* if only **target.cfg** is given, processing will be performed on SQL-level on the database server,
  and only the differences will be fetched to your local machine.

Choose the option that is better for your particular case performance-wise.

## How to compile?

### Requirements

To compile dbdpp, CMake build system is recommended. If not available,
you can still compile it manually—after all, it’s just a single C++ source file **dbdpp.cpp**.

Also, both MySQL client development headers as well as MySQL++ headers should be installed.
These dependencies can be procured from a default software repository; e.g. in Ubuntu

```
sudo apt install libmysqlclient-dev libmysql++-dev
```

Also, you will need a modern C++ compiler with support for C++17 standard.

### Unix-family OS

The easiest way is to run

	cmake .

followed by

	make

in the directory where you cloned your repository; you can also do an
out-of-source build, if you prefer. If successful, binary file **dbdpp** shall
appear.

## Disclaimer

dbdpp is free software; you can redistribute it and/or modify it under the terms
of the GNU General Public License as published by the Free Software Foundation;
either version 2 of the License, or (at your option) any later version.

dbdpp is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
dbdpp (file “LICENCE”); if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
