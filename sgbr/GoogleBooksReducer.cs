using CommandLine;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SGBR.Filters;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SGBR
{
    internal class GoogleBooksReducer
    {
        public class CommonNgramOptions
        {
            [Option('i', "input-file", Required = true, HelpText = "Google Ngram file (gz, 2020 format) or Reduced Ngram File (gz).", SetName = "file")]
            public string InputFile { get; set; }

            [Option('u', "input-url", Required = true, HelpText = "Google Ngram URL (gz, 2020 format) or Reduced Ngram URL (gz).", SetName = "url")]
            public string InputUrl { get; set; }

            [Option('d', "input-dir", Required = true, HelpText = "Directory with Google Ngram (gz, 2020 format) files or Reduced Ngram (gz) files.", SetName = "dir")]
            public string InputDir { get; set; }

            [Option('l', "length-filter", Required = false, HelpText = "Remove lines where any word in ngram has length longer than specified.")]
            public int LengthFilter { get; set; }

            [Option('t', "tag-filter", Required = false, HelpText = "Remove lines with specific tags from the output [NVJARDPMCT.XSE_].")]
            public string TagsFilter { get; set; }

            [Option('n', "no-letters-filter", Required = false, HelpText = "Remove lines where any word has no letters.")]
            public bool NoLettersFilter { get; set; }

            [Option('p', "processing-threads", Required = false, HelpText = "Number of threads used for data processing.")]
            public byte ProcessingThreads { get; set; } = 1;

            [Option('g', "log-level", Required = false, HelpText = "Logging level (Error | Information | Verbose).")]
            public LogEventLevel LogLevel { get; set; } = LogEventLevel.Information;
        }

        [Verb("reduce", HelpText = "Reduce Ngram.")]
        public class ReduceNgramOptions : CommonNgramOptions
        {
            [Option('o', "output-file", Required = true, HelpText = "Reduced Ngram Output file (gz).")]
            public string OutputFile { get; set; }
        }

        [Verb("save-to-sql", HelpText = "Save to SQL Server Database.")]
        public class SaveToSqlOptions : CommonNgramOptions
        {
            [Option('c', "sql-connection-string", Required = true, HelpText = "MS SQL Server connection string.")]
            public string ConnectionString { get; set; }

            [Option('w', "words-table-name", Required = false, HelpText = "Words SQL table name.")]
            public string WordsTableName { get; set; } = "Words";

            [Option('s', "stats-prefix", Required = false, HelpText = "Ngram statistics SQL table/view name prefix.")]
            public string StatsTablePrefix { get; set; } = "Ngram";
        }

        private static async Task<int> Main(string[] args)
        {
            return await new Parser(config =>
            {
                config.CaseInsensitiveEnumValues = true;
                config.HelpWriter = Console.Error;
            })
                   .ParseArguments<ReduceNgramOptions, SaveToSqlOptions>(args)
                   .MapResult(
                        (ReduceNgramOptions opts) => ReduceNgramAsync(opts),
                        (SaveToSqlOptions opts) => SaveToSqlAsync(opts),
                        errs => Task.FromResult(1)
                    );
        }

        private static async Task<int> ReduceNgramAsync(ReduceNgramOptions options)
        {
            var log = CreateLogger(options);
            try
            {
                using var ngramFileWriter = new NgramFileWriter(options.OutputFile, log);
                return await ProcessNgramAsync(options, ngramFileWriter, log);
            }
            catch (Exception e)
            {
                log.Error(e.Message);
                return 1;
            }
        }

        private static async Task<int> SaveToSqlAsync(SaveToSqlOptions options)
        {
            var log = CreateLogger(options);
            try
            {
                using var ngramDatabaseWriter = new NgramDatabaseWriter(options.ConnectionString, options.WordsTableName, options.StatsTablePrefix, log);
                return await ProcessNgramAsync(options, ngramDatabaseWriter, log);
            }
            catch (Exception e)
            {
                log.Error(e.Message);
                return 1;
            }
        }

        private static Logger CreateLogger(CommonNgramOptions options)
        {
            var levelSwitch = new LoggingLevelSwitch { MinimumLevel = options.LogLevel };
            return new LoggerConfiguration().
                MinimumLevel.ControlledBy(levelSwitch).
                WriteTo.Console().
                CreateLogger();
        }

        private static async Task<int> ProcessNgramAsync(CommonNgramOptions options, INgramStatsProcessor ngramStatsProcessor, ILogger log)
        {
            var ngramFilters = CreateNgramFilters(options);
            var ngramReducer = new NgramReducer(ngramFilters, options.ProcessingThreads, ngramStatsProcessor, log);
            var ngramReader = new NgramReader(ngramReducer, log);

            if (options.InputFile != null)
                ngramReader.ProcessGzipFile(options.InputFile);
            else if (options.InputUrl != null)
                await ngramReader.ProcessGzipUrl(options.InputUrl);
            else if (options.InputDir != null)
                ngramReader.ProcessGzipDir(options.InputDir);

            return 0;
        }

        private static IEnumerable<INgramFilter> CreateNgramFilters(CommonNgramOptions options)
        {
            var ngramFilters = new List<INgramFilter>();

            if (options.LengthFilter > 0)
                ngramFilters.Add(new WordLengthFilter(options.LengthFilter));

            if (options.TagsFilter != null)
                ngramFilters.Add(new ContainsTagsFilter(options.TagsFilter));

            if (options.NoLettersFilter)
                ngramFilters.Add(new HasNoLettersFilter());

            return ngramFilters;
        }
    }
}