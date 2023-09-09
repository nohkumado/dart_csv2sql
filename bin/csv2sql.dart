import 'dart:ffi';
import 'dart:io';
import 'package:args/args.dart';
import 'dart:convert';
import 'package:csv/csv.dart';

void main(List<String> args) async {
  final argParser = ArgParser()
    ..addOption('table',
        abbr: 't',
        defaultsTo: 'your_table_name',
        help: 'Specify the table name (default: your_table_name)')
    ..addOption('output',
        abbr: 'o',
        defaultsTo: 'csv2sql.sql',
        help: 'Specify the output SQL file (default: csv2sql.sql)')
    ..addFlag('help',
        abbr: 'h', help: 'Show this help message', negatable: false)
    ..addFlag('batch',
        abbr: 'b',
        defaultsTo: false,
        help: 'create a SQL batch for faster processing',
        negatable: false);

  final argResults = argParser.parse(args);

  if (argResults['help']) {
    print('Usage: dart csv2sql.dart [options] <csv_file>');
    print(argParser.usage);
    exit(0);
  }

  final tableName = argResults['table'] ?? 'your_table_name';
  final outputFileName = argResults['output'] ?? 'csv2sql.sql';

  if (argResults.rest.isEmpty || argResults.rest.length > 1) {
    print('Usage: dart csv2sql.dart [options] <csv_file>');
    print(argParser.usage);
    exit(1);
  }

  final csvFileName = argResults.rest[0];
  final csvFile = File(csvFileName);

  try {
    final input = csvFile.openRead();

    final fields = await input
        .transform(utf8.decoder)
        .transform(CsvToListConverter(eol: '\n', fieldDelimiter: ';'))
        //.transform(const CsvToListConverter())
        .toList();

    // Extract column names and cast them to List<String>
    // Extract column names and convert them to lowercase
    final originalColumnNames = fields.first.cast<String>();
    final columnNames = originalColumnNames
        .map(
            (name) => name.trim().replaceAll(RegExp(r'\s+'), '_').toLowerCase())
        .toList();

    final typesLine = fields[1]; // Assuming types are in the second line
    final conflictColumns = ['nom', 'prenom', 'naissance'];

    final createTableCommand = generateCreateTableCommand(
        tableName, columnNames, typesLine, conflictColumns);

    final output = File(outputFileName);
    final sink = output.openWrite();

    sink.writeln(createTableCommand);

    // Write the batch upsert statement
    //sink.writeln(generateBatchUpsertCommand(tableName, columnNames, conflictColumns));

    final batchInsertStatements = <String>[];

    for (var i = 1; i < fields.length; i++) {
      final values = fields[i];
      final sanitizedValues = values.map(sanitizeSqlValue).toList();
      final upsertCommand =
          //         generateUpsertCommand(tableName, columnNames, values, conflictColumns);
          (argResults['batch'])
              ? generateUpsertValues(tableName, columnNames, sanitizedValues,
                  conflictColumns, typesLine)
              : generateSingleUpsertCommand(
                  tableName, columnNames, sanitizedValues, conflictColumns);
      ;
      //batchInsertStatements.add(upsertCommand);
      batchInsertStatements.add(upsertCommand);

      if (!argResults['batch']) {
        // Write the batch to the file when it reaches a certain size (e.g., 1000)
        if (batchInsertStatements.length >= 1000) {
          sink.writeln(batchInsertStatements.join('\n'));
          batchInsertStatements.clear();
        }
      }
//sink.writeln(upsertCommand);
    }
    // Write any remaining statements in the batch
    if (argResults['batch']) {
      // Generate the batch upsert statement
      String batchUpsertCommand = generateBatchUpsertCommand(
          tableName, columnNames, batchInsertStatements, conflictColumns);
      if (batchInsertStatements.isNotEmpty) {
        sink.writeln(batchUpsertCommand);
      }
    } else if (batchInsertStatements.isNotEmpty) {
      sink.writeln(batchInsertStatements.join('\n'));
    }

    await sink.flush();
    await sink.close();

    print('SQL commands written to $outputFileName');
  } catch (e) {
    print('An error occurred: $e');
  }
}

String generateCreateTableCommand(String tableName, List<String> columnNames,
    List<dynamic> values, List<String> conflictColumns) {
  final columns = <String>[];

  // Assuming the first data line provides the data types
  for (var i = 0; i < columnNames.length; i++) {
    final columnName =
        columnNames[i].trim().replaceAll(RegExp(r'\s+'), '_').toLowerCase();

    final conflictColumnsStr =
        conflictColumns.map((name) => "'$name'").join(', ');
    final value = values[i];

    String columnType;

    // Determine the column type based on the value in the first data line
    if (value is int) {
      columnType = 'INTEGER';
    } else if (value is double) {
      columnType = 'NUMERIC';
    } else if (value is bool) {
      columnType = 'BOOLEAN';
    } else if (value is DateTime) {
      columnType = 'DATE';
    } else {
      columnType = 'TEXT';
    }

    final columnDefinition = '$columnName $columnType';
    columns.add(columnDefinition);
  }

  final conflictColumnsStr = conflictColumns.join(', ');
  final createTableCommand = '''
    CREATE TABLE IF NOT EXISTS $tableName (
      id SERIAL PRIMARY KEY,
      ${columns.join(',\n      ')},
      CONSTRAINT unique_${tableName}_uniques UNIQUE ($conflictColumnsStr)
    );
  ''';

  return createTableCommand;
}

String generateUpsertValues(
    String tableName,
    List<String> columnNames,
    List<dynamic> values,
    List<String> conflictColumns,
    List<dynamic> datatypes) {
  final columnValues = <String>[];
  /*final columnValues = values.map((value) {
    if (value is String) {
      return "'${value.replaceAll("'", "")}'"; // Remove single and double quotes from strings and enclose in single quotes
    } else {
      return '$value'; // No quotes for non-strings (e.g., int, bool)
    }
  }).join(', ');
  */

  for (var i = 0; i < values.length; i++) {
    final value = values[i];
    final datatype = datatypes[i];

    if ("$value".isEmpty) {
      columnValues.add("NULL");
    } else if (value is String) {
      if (datatype == 'int') {
        columnValues.add(value);
      } else {
        columnValues
            .add("'${value.trim()}'"); // Enclose strings in single quotes
      }
    } else if (value == null) {
      columnValues.add('NULL');
    } else {
      columnValues.add('$value'); // No quotes for non-strings (e.g., int, bool)
    }
  }

  final conflictColumnsStr = conflictColumns.join(', ');

  // Use the UPDATE statement to update the existing record on conflict
  final upsertCommand = ''' (${columnValues.join(',')}) ''';
  return upsertCommand;
}

String generateUpsertCommand(String tableName, List<String> columnNames,
    List<dynamic> values, List<String> conflictColumns) {
  final columnValues = values.map((value) {
    if (value is String) {
      return "'${value.replaceAll("'", "")}'"; // Remove single and double quotes from strings and enclose in single quotes
    } else {
      return '$value'; // No quotes for non-strings (e.g., int, bool)
    }
  }).join(', ');

  final conflictColumnsStr = conflictColumns.join(', ');

  // Use the UPDATE statement to update the existing record on conflict
  final upsertCommand = '''
    INSERT INTO $tableName (${columnNames.join(', ')})
    VALUES ($columnValues)
    ON CONFLICT ($conflictColumnsStr)
    DO UPDATE SET (${columnNames.join(', ')}) = ($columnValues);
  ''';

  return upsertCommand;
}

String generateInsertCommand(String tableName, String columnNames,
    List<dynamic> values, List<String> conflictColumns) {
  final columnValues = values.map((value) {
    if (value != null && value is String) {
      return "'${value.replaceAll("'", "''")}'"; // Escape single quotes
    } else if (value != null && value is int) {
      return "${value}"; //
    } else {
      return 'NULL';
    }
  }).join(', ');

  return 'INSERT INTO $tableName (${columnNames}) VALUES ($columnValues);';
  //   return 'INSERT INTO $tableName ($columnNames) VALUES ($columnValues) ON CONFLICT ($conflictColumnsStr) DO NOTHING;';
}

String generateSingleUpsertCommand(String tableName, List<String> columnNames,
    List<dynamic> values, List<String> conflictColumns) {
  final quotedColumnNames = columnNames.map((name) => '"$name"').join(', ');
  final conflictColumnsStr =
      conflictColumns.map((name) => '"$name"').join(', ');
  final columnValues = values.map((value) {
    if ("$value".isEmpty) {
      return 'NULL';
    } else if (value is String) {
      return "'${value.replaceAll("'", "")}'"; // Remove single and double quotes from strings and enclose in single quotes
    } else {
      return '$value'; // No quotes for non-strings (e.g., int, bool)
    }
  }).join(', ');

  // Generate the individual upsert statement
  final upsertCommand = '''
    INSERT INTO $tableName ($quotedColumnNames)
    VALUES ($columnValues)
    ON CONFLICT ($conflictColumnsStr)
    DO UPDATE SET
  ''';

  // Generate the SET clauses to update non-conflict fields without enclosing column names in single quotes
  final nonConflictColumns =
      columnNames.where((name) => !conflictColumns.contains(name));
  final setClauses =
      nonConflictColumns.map((name) => '$name = EXCLUDED.$name').join(', ');

  return '$upsertCommand $setClauses;';
}

String generateBatchUpsertCommand(String tableName, List<String> columnNames,
    List<String> upsertStatements, List<String> conflictColumns) {
  final quotedColumnNames = columnNames.map((name) => '"$name"').join(', ');
  final conflictColumnsStr =
      conflictColumns.map((name) => '"$name"').join(', ');

  // Generate the batch upsert statement
  final batchUpsertCommand = '''
    INSERT INTO $tableName ($quotedColumnNames)
    VALUES (
    ${upsertStatements.join(',\n')}
    )
    ON CONFLICT ($conflictColumnsStr)
    DO UPDATE SET
  ''';

  // Generate the SET clauses to update non-conflict fields without enclosing column names in single quotes
  final nonConflictColumns =
      columnNames.where((name) => !conflictColumns.contains(name));
  final setClauses =
      nonConflictColumns.map((name) => '$name = EXCLUDED.$name').join(', ');

  return '$batchUpsertCommand $setClauses;';
}

dynamic sanitizeSqlValue(dynamic value) {
  //print("sanitizing $value");
  if (value == null) {
    return 'NULL';
  } else if (value is String) {
    // Escape single quotes by doubling them
    return "${value.replaceAll("'", "''")}";
  } else {
    return value;
  }
}
