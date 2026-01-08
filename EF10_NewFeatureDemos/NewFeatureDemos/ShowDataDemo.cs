using EF10_NewFeatureDemos.ConsoleHelpers;
using EF10_NewFeaturesDbLibrary;
using EF10_NewFeaturesModels;
using Microsoft.EntityFrameworkCore;

namespace EF10_NewFeatureDemos.NewFeatureDemos;

public class ShowDataDemo : IAsyncDemo
{
    private readonly InventoryDbContext _db;

    public ShowDataDemo(InventoryDbContext db)
    {
        _db = db;   
    }

    public async Task RunAsync()
    {
        Console.WriteLine("Showing Data...");

        await Run1();    //use for small recordsets and/or Low to Medium Fan Out
        //await Run2();  //use for LARGE recordsets or HIGH FAN OUT (Many genres and contributors for all the items)

        Console.WriteLine("Press any key to continue");
        Console.ReadKey();
    }

    private async Task Run1()
    {
        var itemsData = await _db.Items
            .AsNoTracking()
            .Select(i => new
            {
                i.Id,
                i.ItemName,
                CategoryName = i.Category.CategoryName,
                i.IsOnSale,

                // Correlated subquery -> STRING_AGG (no DISTINCT)
                ContributorsCsv = string.Join(", ",
                    i.ItemContributors
                    .Where(ic => ic.ContributorId != null && ic.Contributor != null)
                    .Select(ic => ic.Contributor!.ContributorName)
                    .Distinct()  // <-- only works if you're on SQL 2022+
                ),

                GenresCsv = string.Join(", ",
                    i.Genres
                    .Select(g => g.GenreName)
                    .Distinct()  // <-- only works if you're on SQL 2022+
                )
            })
            .ToListAsync();

        Console.WriteLine(
            ConsolePrinter.PrintBoxedList(
                itemsData, i => $"{i.Id} - {i.ItemName} - {i.CategoryName} - {i.IsOnSale} - " +
                                    $"{(string.IsNullOrWhiteSpace(i.GenresCsv) ? "No Genres" : i.GenresCsv)} - " +
                                    $"{(string.IsNullOrWhiteSpace(i.ContributorsCsv) ? "No Contributors" : i.ContributorsCsv)}"));

        //-----------------------------------------------------------------
        Console.WriteLine($"Number of items: {itemsData.Count}");
        Console.WriteLine("Items shown successfully.");
    }

    private async Task Run2()
    {
        //use aggregates if you have a very large dataset
        var contributorAgg = _db.ItemContributors
            .Where(ic => ic.Contributor != null)
            .Select(ic => new
            {
                ic.ItemId,
                Name = ic.Contributor!.ContributorName
            })
            .Distinct()
            .GroupBy(x => x.ItemId)
            .Select(g => new
            {
                ItemId = g.Key,
                ContributorsCsv = string.Join(", ",
                    g.Select(x => x.Name))
            });



        var genreAgg = _db.Items
            .SelectMany(i => i.Genres, (i, g) => new
            {
                ItemId = i.Id,
                g.GenreName
            })
            .Distinct()
            .GroupBy(x => x.ItemId)
            .Select(g => new
            {
                ItemId = g.Key,
                GenresCsv = string.Join(", ",
                    g.Select(x => x.GenreName))
            });



        var itemsData = await _db.Items
            .AsNoTracking()
            .Select(i => new
            {
                i.Id,
                i.ItemName,
                CategoryName = i.Category.CategoryName,
                i.IsOnSale
            })
            .GroupJoin(
                contributorAgg,
                i => i.Id,
                c => c.ItemId,
                (i, c) => new { i, c }
            )
            .SelectMany(
                x => x.c.DefaultIfEmpty(),
                (x, c) => new
                {
                    x.i.Id,
                    x.i.ItemName,
                    x.i.CategoryName,
                    x.i.IsOnSale,
                    ContributorsCsv = c!.ContributorsCsv
                }
            )
            .GroupJoin(
                genreAgg,
                i => i.Id,
                g => g.ItemId,
                (i, g) => new { i, g }
            )
            .SelectMany(
                x => x.g.DefaultIfEmpty(),
                (x, g) => new
                {
                    x.i.Id,
                    x.i.ItemName,
                    x.i.CategoryName,
                    x.i.IsOnSale,
                    x.i.ContributorsCsv,
                    GenresCsv = g!.GenresCsv
                }
            )
            .ToListAsync();

        Console.WriteLine(
            ConsolePrinter.PrintBoxedList(
                itemsData, i => $"{i.Id} - {i.ItemName} - {i.CategoryName} - {i.IsOnSale} - " +
                                    $"{(string.IsNullOrWhiteSpace(i.GenresCsv) ? "No Genres" : i.GenresCsv)} - " +
                                    $"{(string.IsNullOrWhiteSpace(i.ContributorsCsv) ? "No Contributors" : i.ContributorsCsv)}"));

        //-----------------------------------------------------------------
        Console.WriteLine($"Number of items: {itemsData.Count}");
        Console.WriteLine("Items shown successfully.");
    }

}
/*
    Why Run1 is faster under 10k (exact reasons)
        1. Lower fixed overhead

            Run 2 always pays for:

            - Multiple grouped subqueries

            - Hashing / sorting for GROUP BY

            - Extra joins back to Items

            - That overhead exists even if you only return 50 rows.

            Run 1:

            - Does simple correlated APPLYs

            - Avoids global grouping

            - Touches less memory up front

            - For small sets, fixed overhead dominates — and Run 1 wins.

        2. SQL Server optimizes APPLY well for small outer sets

            For <10k rows:

                - SQL Server typically uses nested loops

                - Each APPLY hits a narrow index seek

                - Cache locality is excellent

                - So the "per-item" cost in Run 1 stays small.
        3. Fewer execution plan operators

            Plan shape for Run 1:

            - Scan Items

            - Nested-loop APPLY

            - Stream small rowsets

            Plan shape for Run 2:

            - Scan contributors

            - DISTINCT

            - GROUP BY

            - STRING_AGG

            - Hash joins back to Items

            - More operators = more setup cost.

    When Run 2 overtakes Run 1

        Once you cross roughly:

        - 10k–20k items

        - OR high fan-out (many contributors/genres per item)

        - OR endpoint called frequently

        Then Run 1's repeated work grows linearly and B's single-pass aggregation becomes cheaper.
    */