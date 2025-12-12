using DuckDB.NET.Data;
using DuckDB_Experimentation.Setup;
using Xunit.Abstractions;

namespace DuckDB_Experimentation.Tests;

public class DuckDbTests
{
    private readonly ITestOutputHelper output;

    private const string WORKING_DIRECTORY = @"C:\mounts\DuckDBTests";
    private const string FIRST_TEST_DATA_PATH = @"C:\mounts\DealerInvoiceData";
    private const string SECOND_TEST_DATA_PATH = @"C:\mounts\DealerInvoiceData2";

    public DuckDbTests(ITestOutputHelper output)
    {
        this.output = output;

        Directory.CreateDirectory(WORKING_DIRECTORY);
    }

    [Fact]
    public void CreateFirstVehiclesDb()
    {
        DuckDBUtil.CreateVehiclesDatabase(
            workingDirecotyr: WORKING_DIRECTORY,
            duckDbFileName: "FirstVehiclesDatabase.duckdb",
            testDataPath: FIRST_TEST_DATA_PATH
        );
    }

    [Fact]
    public void CreateSecondVehiclesDb()
    {
        DuckDBUtil.CreateVehiclesDatabase(
            workingDirecotyr: WORKING_DIRECTORY,
            duckDbFileName: "SecondVehiclesDatabase.duckdb",
            testDataPath: SECOND_TEST_DATA_PATH
        );
    }

    [Fact]
    public void CreateMasterDb()
    {
        DuckDBUtil.CreateMasterDatabase(
            workingDirecotyr: WORKING_DIRECTORY,
            duckDbFileName: "MasterDatabase.duckdb",
            testDataPath: FIRST_TEST_DATA_PATH
        );
    }

    [Fact]
    public void JoinVehicleDataByVIN()
    {
        var conn = new DuckDBConnection($@"DataSource={Path.Combine(WORKING_DIRECTORY, "MasterDatabase.duckdb")}");
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

    [Fact]
    public void DiffDuckDBFiles()
    {
        var previousDatabasePath = Path.Combine(WORKING_DIRECTORY, "FirstVehiclesDatabase.duckdb");
        var currentDatabasePath = Path.Combine(WORKING_DIRECTORY, "SecondVehiclesDatabase.duckdb");

        Assert.True(File.Exists(currentDatabasePath));
        Assert.True(File.Exists(currentDatabasePath));

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
                SELECT * FROM previous.Vehicle
            ";

            using var reader = cmd.ExecuteReader();
            output.WriteLine("DIFF: Vehicle");
            while (reader.Read())
            {
                this.output.WriteLine(reader["VIN"].ToString()); // Print the changed VINs
            }
        }
    }
}