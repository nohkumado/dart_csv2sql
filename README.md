# dart_csv2sql
My own implementation of a csv to sql script, as usual the other projects i tried did not
work out of the box, and instead of delving into foreign code i preferred write my own version :D

written in 2h, so don't expect too much in code quaility, documentation and regression tests, but it 
does the job i need it for... so maybe it can be servicable for others...

## running the program 


```
    dart bin/csv2sql.dart -h
    Usage: dart csv2sql.dart [options] <csv_file>
    -t, --table     Specify the table name (default: your_table_name)
                    (defaults to "your_table_name")
    -o, --output    Specify the output SQL file (default: csv2sql.sql)
                    (defaults to "csv2sql.sql")
    -h, --help      Show this help message
    -b, --batch     create a SQL batch for faster processing
```

## compiling the program

for better run times, you can compile the dart code instead of the jit compiler with 

```dart compile exe bin/csv2sql.dart```

## Licence etc.

if you are happy using this, let me know, contributions allways welcome
