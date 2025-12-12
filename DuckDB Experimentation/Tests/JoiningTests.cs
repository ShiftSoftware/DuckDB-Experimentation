using DuckDB.NET.Data;
using DuckDB_Experimentation.Setup;
using Xunit.Abstractions;

namespace DuckDB_Experimentation.Tests;

[TestCaseOrderer(
    "DuckDB_Experimentation.Tests.PriorityOrderer",
    "DuckDB Experimentation"
)]
public class JoiningTests
{
    private readonly ITestOutputHelper output;

    public JoiningTests(ITestOutputHelper output)
    {
        this.output = output;

        Directory.CreateDirectory(Constants.WORKING_DIRECTORY);
    }


    [Fact(DisplayName = "04. Create Master Database")]
    [TestPriority(1)]
    public void CreateMasterDb()
    {
        DuckDBUtil.CreateMasterDatabase(
            workingDirecotyr: Constants.WORKING_DIRECTORY,
            duckDbFileName: "MasterDatabase.duckdb",
            testDataPath: Constants.FIRST_TEST_DATA_PATH
        );
    }


    [Fact(DisplayName = "05. Join Vehicles by VIN")]
    [TestPriority(2)]
    public void JoinVehicleDataByVIN()
    {
        var conn = new DuckDBConnection($@"DataSource={Path.Combine(Constants.WORKING_DIRECTORY, "MasterDatabase.duckdb")}");
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

        // --- Move to Third Result Set: Part ---
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
}