using BenchmarkDotNet.Disassemblers;
using CsvHelper;
using CsvHelper.Configuration;
using Npgsql;
using NpgsqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.Intrinsics.Arm;
using static System.Net.Mime.MediaTypeNames;

namespace DuckDB_Experimentation.Setup;

public class PostgresUtil
{
    public static void InsertCSVToPostgress()
    {
        //CREATE TABLE Part(
        //    InvoiceStatus TEXT,
        //    OrderStatus TEXT,
        //    DateLastEditted TEXT,
        //    SOPartsData_DealerId TEXT,
        //    COMP TEXT,
        //    WIPNumber TEXT,
        //    InvoiceNumber TEXT,
        //    OrderQuantity TEXT,
        //    SaleType TEXT,
        //    AccountNo TEXT,
        //    MenuCode TEXT,
        //    ExtendedPrice TEXT,
        //    PartNumber TEXT,
        //    LineNumber TEXT,
        //    OriginalInvoice TEXT,
        //    Customer_Magic TEXT,
        //    Dep TEXT,
        //    VIN TEXT,
        //    DateCreated TEXT,
        //    DateInserted TEXT
        //);


        Console.WriteLine($"Current Working Set (MB): {Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0:F2}");

        var stopWatch = new Stopwatch();

        long beforeMemory = GC.GetTotalMemory(forceFullCollection: true);

        stopWatch.Start();

        var connectionString = "Host=localhost;Username=postgres;Password=0802a2;Database=DealerData";
        var csvPath = @"C:\mounts\AllPartsData.csv";

        Console.WriteLine("Starting Import");

        using var conn = new NpgsqlConnection(connectionString);
        conn.Open();

        using var writer = conn.BeginBinaryImport(@"
            COPY Part (
                InvoiceStatus,
                OrderStatus,
                DateLastEditted,
                SOPartsData_DealerId,
                COMP,
                WIPNumber,
                InvoiceNumber,
                OrderQuantity,
                SaleType,
                AccountNo,
                MenuCode,
                ExtendedPrice,
                PartNumber,
                LineNumber,
                OriginalInvoice,
                Customer_Magic,
                Dep,
                VIN,
                DateCreated,
                DateInserted
            ) FROM STDIN (FORMAT BINARY)");

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            BadDataFound = null,
            MissingFieldFound = null,
            IgnoreBlankLines = true,
            TrimOptions = TrimOptions.Trim,
        };

        using var reader = new StreamReader(csvPath);
        using var csv = new CsvReader(reader, config);

        csv.Read();
        csv.ReadHeader();

        while (csv.Read())
        {
            writer.StartRow();

            writer.Write(csv.GetField("InvoiceStatus"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("OrderStatus"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("DateLastEditted"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("SOPartsData_DealerId"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("COMP"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("WIPNumber"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("InvoiceNumber"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("OrderQuantity"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("SaleType"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("AccountNo"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("MenuCode"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("ExtendedPrice"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("PartNumber"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("LineNumber"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("OriginalInvoice"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("Customer_Magic"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("Dep"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("VIN"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("DateCreated"), NpgsqlDbType.Text);
            writer.Write(csv.GetField("DateInserted"), NpgsqlDbType.Text);
        }

        writer.Complete();

        stopWatch.Stop();

        long afterMemory = GC.GetTotalMemory(forceFullCollection: true);

        Console.WriteLine("CSV data inserted successfully within {0}", stopWatch.Elapsed.ToString());

        Console.WriteLine($"Memory used: {(afterMemory - beforeMemory) / 1024.0 / 1024.0:F2} MB");

        Console.WriteLine($"Working Set (MB): {Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0:F2}");
    }
}