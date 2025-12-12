using DuckDB_Experimentation.Setup;
using Xunit.Abstractions;

namespace DuckDB_Experimentation.Tests;

public class JPM
{
    private readonly ITestOutputHelper output;

    public JPM(ITestOutputHelper output)
    {
        this.output = output;

        Directory.CreateDirectory(Constants.WORKING_DIRECTORY);
    }

    [Fact(DisplayName = "01. Create JPM Parquet")]
    public void CreateJPMParquet()
    {
        DuckDBUtil.CreateJPMParquet(
            workingDirecotyr: Constants.WORKING_DIRECTORY,
            parquetFileName: "JPM.parquet",
            sourceCSVFile: Constants.JPM_CSV_PATH
        );
    }
}