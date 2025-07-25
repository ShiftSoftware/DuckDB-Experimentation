using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

[SimpleJob(warmupCount: 1, iterationCount: 50)]
[MemoryDiagnoser]
public class DuckDBDiffBenchmark
{
    private string previousDatabasePath = default!;
    private string currentDatabasePath = default!;

    [GlobalSetup]
    public void Setup()
    {
        previousDatabasePath = @"C:\mounts\DealerInvoiceData_2025-07-25-15-00-00.duckdb";
        currentDatabasePath = @"C:\mounts\DealerInvoiceData_2025-07-25-15-35-29.duckdb";
    }

    [Benchmark]
    public void Diff()
    {
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                ATTACH '{previousDatabasePath.Replace(@"\", @"\\")}' AS previous;
                ATTACH '{currentDatabasePath.Replace(@"\", @"\\")}' AS current;";
            cmd.ExecuteNonQuery();
        }

        DiffTable(conn, "Vehicle");
        DiffTable(conn, "Labor");
        DiffTable(conn, "Part");
    }

    private void DiffTable(DuckDBConnection conn, string tableName)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
            SELECT * FROM current.{tableName}
            EXCEPT
            SELECT * FROM previous.{tableName};
        ";

        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read())
            count++;
    }
}