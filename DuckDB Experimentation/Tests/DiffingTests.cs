using DuckDB.NET.Data;
using DuckDB_Experimentation.Setup;
using Xunit.Abstractions;

namespace DuckDB_Experimentation.Tests;

[TestCaseOrderer(
    "DuckDB_Experimentation.Tests.PriorityOrderer",
    "DuckDB Experimentation"
)]
public class DiffingTests
{
    private readonly ITestOutputHelper output;

    public DiffingTests(ITestOutputHelper output)
    {
        this.output = output;

        Directory.CreateDirectory(Constants.WORKING_DIRECTORY);
    }


    [Fact(DisplayName = "01. Create First Vehicles Database")]
    [TestPriority(1)]
    public void CreateFirstVehiclesDb()
    {
        DuckDBUtil.CreateVehiclesDatabase(
            workingDirecotyr: Constants.WORKING_DIRECTORY,
            duckDbFileName: "FirstVehiclesDatabase.duckdb",
            testDataPath: Constants.FIRST_TEST_DATA_PATH
        );
    }


    [Fact(DisplayName = "02. Create Second Vehicles Database")]
    [TestPriority(1)]
    public void CreateSecondVehiclesDb()
    {
        DuckDBUtil.CreateVehiclesDatabase(
            workingDirecotyr: Constants.WORKING_DIRECTORY,
            duckDbFileName: "SecondVehiclesDatabase.duckdb",
            testDataPath: Constants.SECOND_TEST_DATA_PATH
        );
    }


    [Fact(DisplayName = "03. Diff Vehicles")]
    [TestPriority(2)]
    public void DiffDuckDBFiles()
    {
        var previousDatabasePath = Path.Combine(Constants.WORKING_DIRECTORY, "FirstVehiclesDatabase.duckdb");
        var currentDatabasePath = Path.Combine(Constants.WORKING_DIRECTORY, "SecondVehiclesDatabase.duckdb");

        Assert.True(File.Exists(previousDatabasePath));
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
                this.output.WriteLine(reader["VIN"].ToString());
            }
        }
    }
}