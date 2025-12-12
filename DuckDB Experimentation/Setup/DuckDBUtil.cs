using DuckDB.NET.Data;

namespace DuckDB_Experimentation.Setup;

public class DuckDBUtil
{
    public static DuckDBConnection CreateVehiclesDatabase(
        string workingDirecotyr,
        string duckDbFileName,
        string testDataPath
    )
    {
        var file = Path.Combine(workingDirecotyr, duckDbFileName);

        using var conn = new DuckDBConnection($"DataSource={file}");

        conn.Open();

        var commands = new[]
        {
            "DROP TABLE IF EXISTS Vehicle;",
            //"DROP TABLE IF EXISTS Labor;",
            //"DROP TABLE IF EXISTS Part;",

            // Vehicles
                $"""
                CREATE TABLE Vehicle AS
                  SELECT FullChassis AS VIN,
                         * EXCLUDE FullChassis,
                         'Vehicle' AS Type
                  FROM read_csv(
                      '{testDataPath}\VSDatas_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,

            // Labor
            //    """
            //    CREATE TABLE Labor AS
            //      SELECT *,
            //             'Labor' AS Type
            //      FROM read_csv(
            //          'C:/mounts/DealerInvoiceData/SOLabordatas_Full_*.csv',
            //          delim=',',
            //          header=true,
            //          sample_size=-1
            //      );
            //    """,

            //// Part
            //    """
            //    CREATE TABLE Part AS
            //      SELECT *,
            //             'Part' AS Type
            //      FROM read_csv(
            //          'C:/mounts/DealerInvoiceData/SOPartsDatas_Full_*.csv',
            //          delim=',',
            //          header=true,
            //          sample_size=-1
            //      );
            //    """,
        };

        foreach (var sql in commands)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        return conn;
    }

    public static DuckDBConnection CreateMasterDatabase(
        string workingDirecotyr,
        string duckDbFileName,
        string testDataPath
    )
    {
        var file = Path.Combine(workingDirecotyr, duckDbFileName);

        using var conn = new DuckDBConnection($"DataSource={file}");

        conn.Open();

        var commands = new[]
        {
            "DROP TABLE IF EXISTS Vehicle;",
            "DROP TABLE IF EXISTS Labor;",
            "DROP TABLE IF EXISTS Part;",

            // Vehicles
                $"""
                CREATE TABLE Vehicle AS
                  SELECT FullChassis AS VIN,
                         * EXCLUDE FullChassis,
                         'Vehicle' AS Type
                  FROM read_csv(
                      '{testDataPath}\VSDatas_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,

            // Labor
                $"""
                CREATE TABLE Labor AS
                  SELECT *,
                         'Labor' AS Type
                  FROM read_csv(
                      '{testDataPath}\SOLabordatas_Full_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,

            //// Part
                $"""
                CREATE TABLE Part AS
                  SELECT *,
                         'Part' AS Type
                  FROM read_csv(
                      '{testDataPath}\SOPartsDatas_Full_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,
        };

        foreach (var sql in commands)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        return conn;
    }

    public static void CreateJPMParquet(
        string workingDirecotyr,
        string parquetFileName,
        string sourceCSVFile,
        int maxRecords = -1
    )
    {
        var parquetFile = Path.Combine(workingDirecotyr, parquetFileName);
        var tempDbFile = Path.Combine(workingDirecotyr, "temp_jpm.db");

        try
        {
            using var conn = new DuckDBConnection($"DataSource={tempDbFile}");
            conn.Open();

            var limitClause = maxRecords > 0 ? $"LIMIT {maxRecords}" : "";

            var sql = $"""
                COPY (
                  SELECT *
                  FROM read_csv(
                      '{sourceCSVFile}',
                      delim=';',
                      header=true,
                      sample_size=-1
                  )
                  {limitClause}
                ) TO '{parquetFile}' (FORMAT PARQUET);
                """;

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        finally
        {
            if (File.Exists(tempDbFile))
            {
                File.Delete(tempDbFile);
            }
        }
    }
}