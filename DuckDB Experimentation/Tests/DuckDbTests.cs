using DuckDB.NET.Data;
using DuckDB_Experimentation.Setup;
using Xunit.Abstractions;

namespace DuckDB_Experimentation.Tests;

public class DuckDbTests
{
    private readonly ITestOutputHelper output;

    public DuckDbTests(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    public void CreateDb()
    {
        DuckDBUtil.CreateDuckDBDatabase(@$"DataSource=C:\mounts\DealerInvoiceData_{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss")}.duckdb");
    }

    [Fact]
    public void JoinVehicleDataByVIN()
    {
        var conn = new DuckDBConnection(@"DataSource=C:\mounts\DealerInvoiceData.duckdb");
        conn.Open();

        var sql = $@"
            SELECT * FROM Vehicle WHERE VIN = 'MR0AX8CD0R4447125';
            SELECT * FROM Labor WHERE VIN = 'MR0AX8CD0R4447125';
            SELECT * FROM Part WHERE VIN = 'MR0AX8CD0R4447125';
        ";

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();

        // --- First Result Set: Vehicles ---
        this.output.WriteLine("Vehicle:");
        while (reader.Read())
        {
            var model = reader["Katashki"];
            this.output.WriteLine($"Katashki: {model}");
        }

        // --- Move to Second Result Set: Labor ---
        if (reader.NextResult())
        {
            this.output.WriteLine("");
            this.output.WriteLine("");
            this.output.WriteLine("Labor:");
            while (reader.Read())
            {
                var description = reader["Description"];
                this.output.WriteLine($"Description: {description}");
            }
        }

        // --- Move to Second Result Set: Labor ---
        if (reader.NextResult())
        {
            this.output.WriteLine("");
            this.output.WriteLine("");
            this.output.WriteLine("Part:");
            while (reader.Read())
            {
                var partNumber = reader["PartNumber"];
                this.output.WriteLine($"PartNumber: {partNumber}");
            }
        }
    }

    private string GetPreviousDatabaseFile()
    {
        return @"DataSource=C:\mounts\DealerInvoiceData_2025-07-25-15-00-00.duckdb";
    }

    [Fact]
    public void DiffDuckDBFiles()
    {
        string previousDatabasePath = "C:\\mounts\\DealerInvoiceData_2025-07-25-15-00-00.duckdb";
        var currentDatabasePath = @"C:\mounts\DealerInvoiceData_2025-07-25-15-35-29.duckdb";

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                ATTACH '{previousDatabasePath.Replace(@"\", @"\\")}' AS previous;
                ATTACH '{currentDatabasePath.Replace(@"\", @"\\")}' AS current;
            ";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
                cmd.CommandText = @"
                SELECT * FROM current.Vehicle
                EXCEPT
                SELECT * FROM previous.Vehicle;
            ";

            using var reader = cmd.ExecuteReader();
            output.WriteLine("DIFF: Vehicle");
            while (reader.Read())
            {
                this.output.WriteLine(reader["VIN"].ToString()); // Print the changed VINs
            }
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT * FROM current.Labor
                EXCEPT
                SELECT * FROM previous.Labor;
            ";

            using var reader = cmd.ExecuteReader();
            output.WriteLine("DIFF: Labor");
            while (reader.Read())
            {
                this.output.WriteLine(reader["VIN"].ToString()); // Print the changed VINs
            }
        }

  
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT * FROM current.Part
                EXCEPT
                SELECT * FROM previous.Part;
            ";

            using var reader = cmd.ExecuteReader();
            output.WriteLine("DIFF: Part");
            while (reader.Read())
            {
                this.output.WriteLine(reader["VIN"].ToString()); // Print the changed VINs
            }
        }
    }
}