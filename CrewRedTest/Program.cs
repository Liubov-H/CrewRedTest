using CsvHelper;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

class Program
{
	private const string CsvFilePath = "D:\\VisualStudioApps\\CrewRedTest\\CrewRedTest\\data_files\\sample-cab-data.csv";
	private const string DuplicatesFilePath = "D:\\VisualStudioApps\\CrewRedTest\\CrewRedTest\\data_files\\duplicates.csv";
	private const string ConnectionString = "Server=(local);Database=CrewRedTest;User Id=sa;Password=sa;TrustServerCertificate=True;";
	private const string DestinationTableName = "sample_cab_data";
	private const string DateTimeFormat = "MM/dd/yyyy hh:mm:ss tt";
	private const long MaxInputFileBytes = 200L * 1024 * 1024; // 200 MB
	private const int MaxRowsToImport = 1_000_000;

	static void Main()
	{
		DataTable dataTable = ReadCsvToDataTable(CsvFilePath);
		BulkInsertToSql(dataTable, ConnectionString, DestinationTableName);
	}

	static DataTable ReadCsvToDataTable(string filePath)
	{
		var fileInfo = new FileInfo(filePath);
		if (!fileInfo.Exists)
			throw new FileNotFoundException("CSV file not found.", filePath);
		if (fileInfo.Length > MaxInputFileBytes)
			throw new InvalidOperationException($"CSV file too large ({fileInfo.Length} bytes). Max allowed is {MaxInputFileBytes} bytes.");

		var dataTable = new DataTable();
		AddColumns(dataTable);

		var estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

		using (var reader = new StreamReader(filePath))
		using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
		{
			csv.Read();
			csv.ReadHeader();

			int imported = 0;
			int rejected = 0;

			while (csv.Read())
			{
				if (imported >= MaxRowsToImport)
					throw new InvalidOperationException($"Row limit exceeded. Max allowed is {MaxRowsToImport} rows.");

				DataRow row = dataTable.NewRow();
				try
				{
					FillRow(row, csv, estZone);
					dataTable.Rows.Add(row);
					imported++;
				}
				catch (Exception)
				{
					// Malformed or unexpected row; skip to keep import resilient to untrusted input.
					rejected++;
				}
			}

			if (rejected > 0)
				Console.WriteLine($"Skipped {rejected} malformed row(s).");
		}

		RemoveAndExportDuplicates(dataTable, DuplicatesFilePath);
		return dataTable;
	}

	static void AddColumns(DataTable dataTable)
	{
		dataTable.Columns.Add(nameof(ColumnHeaders.tpep_pickup_datetime), typeof(DateTime));
		dataTable.Columns.Add(nameof(ColumnHeaders.tpep_dropoff_datetime), typeof(DateTime));
		dataTable.Columns.Add(nameof(ColumnHeaders.passenger_count), typeof(int));
		dataTable.Columns.Add(nameof(ColumnHeaders.trip_distance), typeof(decimal));
		dataTable.Columns.Add(nameof(ColumnHeaders.store_and_fwd_flag), typeof(string));
		dataTable.Columns.Add(nameof(ColumnHeaders.PULocationID), typeof(string));
		dataTable.Columns.Add(nameof(ColumnHeaders.DOLocationID), typeof(string));
		dataTable.Columns.Add(nameof(ColumnHeaders.fare_amount), typeof(decimal));
		dataTable.Columns.Add(nameof(ColumnHeaders.tip_amount), typeof(decimal));
	}

	static void FillRow(DataRow row, CsvReader csv, TimeZoneInfo sourceTimeZone)
	{
		SetDateTimeUtc(row, csv, ColumnHeaders.tpep_pickup_datetime, sourceTimeZone);
		SetDateTimeUtc(row, csv, ColumnHeaders.tpep_dropoff_datetime, sourceTimeZone);
		SetNullableInt(row, csv, ColumnHeaders.passenger_count);
		SetNullableDecimal(row, csv, ColumnHeaders.trip_distance);
		SetStoreAndFwdFlag(row, csv);
		SetTrimmedString(row, csv, ColumnHeaders.PULocationID);
		SetTrimmedString(row, csv, ColumnHeaders.DOLocationID);
		SetNullableDecimal(row, csv, ColumnHeaders.fare_amount);
		SetNullableDecimal(row, csv, ColumnHeaders.tip_amount);
	}

	static void SetDateTimeUtc(DataRow row, CsvReader csv, ColumnHeaders column, TimeZoneInfo sourceTimeZone)
	{
		string value = csv.GetField<string>(column.ToString());
		if (string.IsNullOrWhiteSpace(value))
		{
			row[column.ToString()] = DBNull.Value;
			return;
		}

		if (!DateTime.TryParseExact(value, DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var local))
		{
			row[column.ToString()] = DBNull.Value;
			return;
		}

		row[column.ToString()] = TimeZoneInfo.ConvertTimeToUtc(local, sourceTimeZone);
	}

	static void SetNullableInt(DataRow row, CsvReader csv, ColumnHeaders column)
	{
		string value = csv.GetField<string>(column.ToString());
		if (string.IsNullOrWhiteSpace(value) || !int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
		{
			row[column.ToString()] = DBNull.Value;
			return;
		}

		row[column.ToString()] = parsed;
	}

	static void SetNullableDecimal(DataRow row, CsvReader csv, ColumnHeaders column)
	{
		string value = csv.GetField<string>(column.ToString());
		if (string.IsNullOrWhiteSpace(value) || !decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed))
		{
			row[column.ToString()] = DBNull.Value;
			return;
		}

		row[column.ToString()] = parsed;
	}

	static void SetStoreAndFwdFlag(DataRow row, CsvReader csv)
	{
		string value = csv.GetField<string>(ColumnHeaders.store_and_fwd_flag.ToString());
		if (string.IsNullOrWhiteSpace(value))
		{
			row[ColumnHeaders.store_and_fwd_flag.ToString()] = DBNull.Value;
			return;
		}

		value = value.Trim();
		if (string.Equals(value, "Y", StringComparison.OrdinalIgnoreCase))
		{
			row[ColumnHeaders.store_and_fwd_flag.ToString()] = "Yes";
			return;
		}

		if (string.Equals(value, "N", StringComparison.OrdinalIgnoreCase))
		{
			row[ColumnHeaders.store_and_fwd_flag.ToString()] = "No";
			return;
		}

		row[ColumnHeaders.store_and_fwd_flag.ToString()] = DBNull.Value;
	}

	static void SetTrimmedString(DataRow row, CsvReader csv, ColumnHeaders column)
	{
		string value = csv.GetField<string>(column.ToString());
		row[column.ToString()] = string.IsNullOrWhiteSpace(value) ? DBNull.Value : value.Trim();
	}

	static void RemoveAndExportDuplicates(DataTable dataTable, string duplicatesFilePath)
	{
		var duplicateRows = dataTable.AsEnumerable()
			.GroupBy(row => new
			{
				Pickup = row.Field<DateTime>(nameof(ColumnHeaders.tpep_pickup_datetime)),
				Dropoff = row.Field<DateTime>(nameof(ColumnHeaders.tpep_dropoff_datetime)),
				Passengers = row.Field<int?>(nameof(ColumnHeaders.passenger_count))
			})
			.Where(g => g.Count() > 1)
			.SelectMany(g => g.Skip(1))
			.ToList();

		ExportRowsToCsv(duplicateRows, dataTable, duplicatesFilePath);

		foreach (DataRow row in duplicateRows)
			dataTable.Rows.Remove(row);
	}

	static void ExportRowsToCsv(List<DataRow> rows, DataTable schema, string filePath)
	{
		var columnNames = schema.Columns.Cast<DataColumn>().Select(c => c.ColumnName);
		using var writer = new StreamWriter(filePath);
		writer.WriteLine(string.Join(",", columnNames));
		foreach (DataRow row in rows)
		{
			var fields = row.ItemArray.Select(ToSafeCsvField);
			writer.WriteLine(string.Join(",", fields));
		}
	}

	static string ToSafeCsvField(object? value)
	{
		if (value is null || value == DBNull.Value)
			return string.Empty;

		string s = value.ToString() ?? string.Empty;

		// Prevent CSV injection when opening exported CSV in Excel-like tools.
		if (s.Length > 0 && (s[0] == '=' || s[0] == '+' || s[0] == '-' || s[0] == '@'))
			s = "'" + s;

		// Proper CSV quoting (commas/quotes/newlines).
		bool mustQuote = s.Contains(',') || s.Contains('"') || s.Contains('\n') || s.Contains('\r');
		if (mustQuote)
			s = "\"" + s.Replace("\"", "\"\"") + "\"";

		return s;
	}

	static void BulkInsertToSql(DataTable dataTable, string connectionString, string destinationTable)
	{
		using var connection = new SqlConnection(connectionString);
		connection.Open();

		using var bulkCopy = new SqlBulkCopy(connection)
		{
			DestinationTableName = destinationTable
		};

		foreach (DataColumn column in dataTable.Columns)
			bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

		try
		{
			bulkCopy.WriteToServer(dataTable);
			Console.WriteLine("Data imported successfully!");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"An error occurred: {ex.Message}");
		}
	}

	enum ColumnHeaders
	{
		tpep_pickup_datetime,
		tpep_dropoff_datetime,
		passenger_count,
		trip_distance,
		store_and_fwd_flag,
		PULocationID,
		DOLocationID,
		fare_amount,
		tip_amount
	}
}
