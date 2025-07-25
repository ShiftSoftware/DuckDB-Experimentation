using DuckDB.NET.Data;

namespace DuckDB_Experimentation.Setup;

public class DuckDBUtil
{
    public static DuckDBConnection CreateDuckDBDatabase(string fileName)
    {
        using var conn = new DuckDBConnection(fileName);
        conn.Open();

        var commands = new[]
        {
            "DROP TABLE IF EXISTS Vehicle;",
            "DROP TABLE IF EXISTS Labor;",
            "DROP TABLE IF EXISTS Part;",

            // Vehicles
                """
                CREATE TABLE Vehicle AS
                  SELECT FullChassis AS VIN,
                         * EXCLUDE FullChassis,
                         'Vehicle' AS Type
                  FROM read_csv(
                      'C:/mounts/DealerInvoiceData/VSDatas_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,

            // Labor
                """
                CREATE TABLE Labor AS
                  SELECT *,
                         'Labor' AS Type
                  FROM read_csv(
                      'C:/mounts/DealerInvoiceData/SOLabordatas_Full_*.csv',
                      delim=',',
                      header=true,
                      sample_size=-1
                  );
                """,

            // Part
                """
                CREATE TABLE Part AS
                  SELECT *,
                         'Part' AS Type
                  FROM read_csv(
                      'C:/mounts/DealerInvoiceData/SOPartsDatas_Full_*.csv',
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
}