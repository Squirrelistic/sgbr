using Serilog;
using SGBR.Model;
using SGBR.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SGBR
{
    public class NgramDatabaseWriter : INgramStatsProcessor, IDisposable
    {
        private const int StatsQueueBufferSize = 2131;

        private const int DatabaseSaveBatchSize = 500000;

        // The maximum key length for a MSSQL nonclustered index is 1700 bytes (~850 unicode bytes)
        private const int MaxWordLengthAllowed = 850;

        // Ngram table name = StatsPrefix + NgramStatsTablePosfix + WordsCount e.g. EngStats2
        private const string NgramStatsTablePosfix = "Stats";

        // Ngram table name = StatsPrefix + NgramWordsStatsViewPosfix + WordsCount e.g. EngWordsStats3
        private const string NgramWordsStatsViewPosfix = "WordsStats";

        public const int BulkCopyTimeoutSeconds = 300;

        #region SQL Templates

        private const string CreateWordsTableIfNotExistsSql = @"
            IF OBJECT_ID (N'{0}', N'U') IS NULL
            CREATE TABLE [{0}] (
            	[Id] [int] IDENTITY(1,1) NOT NULL,
            	[Word] [nvarchar]({1}) NOT NULL,
                CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ([Id] ASC)
            )";

        private const string CreateStatsTableIfNotExistsSql = @"
            IF OBJECT_ID (N'{0}', N'U') IS NULL
            CREATE TABLE [{0}] (
{1}
                [Tags] [char]({2}) NOT NULL,
                [MatchCount] [bigint] NOT NULL,
                [VolumeCount] [int] NOT NULL,
                [FirstYear] [smallint] NOT NULL,
                [LastYear] [smallint] NOT NULL,
                [YearCount] [smallint] NOT NULL,
                [TopYear] [smallint] NOT NULL,
                [TopYearMatchCount] [bigint] NOT NULL
                CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ({3}Tags)
            )";

        private const string CheckIfWordsStatsViewExistsSql = "SELECT OBJECT_ID (N'{0}', N'V')";

        private const string CreateWordsStatsViewSql = @"
            CREATE VIEW [{0}] AS
            SELECT
{2}
                [Tags],
                [MatchCount],
                [VolumeCount],
                [FirstYear],
                [LastYear],
                [YearCount],
                [TopYear],
                [TopYearMatchCount]
            FROM [{1}]
{3}";

        private const string CreateStatsWordIdColumnSql = "            	[Word{0}Id] [int] NOT NULL CONSTRAINT FK_{1}_Word{0} REFERENCES {2}(Id),";
        private const string SelectMaxIdSql = "SELECT MAX([Id]) FROM {0}";
        private const string SelectAllWordsSql = "SELECT [Id], [Word] FROM {0}";
        private const string WordsStatsViewSelectSql = "                [Word{0}].[Word] AS [Word{0}],";
        private const string WordsStatsViewJoinSql = "            JOIN [{0}] Word{1} ON Word{1}.Id = Word{1}Id";

        #endregion SQL Templates

        private readonly string _connectionString;
        private readonly string _wordsTableName;
        private readonly string _statsPrefix;
        private readonly ILogger _log;

        private BlockingCollection<NgramTsvStats> _statsQueue;
        private Task _writeToDatabaseTask;
        private bool _isDisposed;

        public NgramDatabaseWriter(string connectionString, string wordsTableName, string statsPrefix, ILogger log)
        {
            _connectionString = connectionString;
            _wordsTableName = wordsTableName;
            _statsPrefix = statsPrefix;
            _log = log;
            InitStatsProcessing();
        }

        public void InitStatsProcessing()
        {
            _statsQueue = new BlockingCollection<NgramTsvStats>(StatsQueueBufferSize);
            _writeToDatabaseTask = Task.Run(() => WriteToDatabaseTask());
        }

        public void ProcessStats(NgramTsvStats stats)
        {
            if (_isDisposed) throw new Exception($"{GetType().Name} object already disposed");

            do ThrowExceptionIfWriteToDatabaseTaskFailed();
            while (!_statsQueue.TryAdd(stats, 1000));
        }

        public void EndStatsProcessing()
        {
            _statsQueue.CompleteAdding();
            _writeToDatabaseTask.Wait();
            ThrowExceptionIfWriteToDatabaseTaskFailed();
        }

        private void WriteToDatabaseTask()
        {
            using var sqlConnection = new SqlConnection(_connectionString);

            _log.Information("Opening connection to database");
            sqlConnection.Open();

            CreateWordsDBTableIfNotExist(sqlConnection);
            var maxNgramWordId = GetMaxIdFromSqlTable(_wordsTableName, sqlConnection);
            var wordToWordId = GetWordsDictionary(sqlConnection);
            var ngramWordsDataTable = CreateNgramWordsDataTable();

            string statsTableName = null;
            DataTable ngramStatsDataTable = null;

            int statCount = 0;
            int wordsCount = 0;
            using var sqlBulk = new SqlBulkCopy(sqlConnection)
            {
                BulkCopyTimeout = BulkCopyTimeoutSeconds
            };
            foreach (var stats in _statsQueue.GetConsumingEnumerable())
            {
                if (statCount++ == 0)
                {
                    wordsCount = stats.Ngram.Length;
                    statsTableName = GetStatsTableName(wordsCount);
                    CreateStatsDBTableIfNotExist(wordsCount, sqlConnection);
                    CreateWordsStatsDBViewIfNotExist(wordsCount, sqlConnection);
                    ngramStatsDataTable = CreateNgramStatsDataTable(wordsCount);
                }

                var wordLongerThanAllowed = stats.Ngram.FirstOrDefault(x => x.Length > MaxWordLengthAllowed);
                if (wordLongerThanAllowed != null)
                {
                    Log.Warning($"Skipping word {wordLongerThanAllowed} because it is longer than {MaxWordLengthAllowed} characters");
                    continue;
                }

                if (stats.Ngram.Length != wordsCount)
                    throw new Exception($"Inconsistent words count in ngram stats: was {wordsCount}, is {stats.Ngram.Length}");

                var dataRowValues = new object[8 + wordsCount];
                var dataColumn = 0;
                foreach (var word in stats.Ngram)
                {
                    if (!wordToWordId.TryGetValue(word, out long statNgramWordId))
                    {
                        statNgramWordId = ++maxNgramWordId;
                        ngramWordsDataTable.Rows.Add(statNgramWordId, word);
                        wordToWordId.Add(word, statNgramWordId);
                    }
                    dataRowValues[dataColumn++] = statNgramWordId;
                }
                dataRowValues[dataColumn++] = WordTagUtils.TagsToString(stats.Tags);
                dataRowValues[dataColumn++] = stats.MatchCount;
                dataRowValues[dataColumn++] = stats.VolumeCount;
                dataRowValues[dataColumn++] = stats.FirstYear;
                dataRowValues[dataColumn++] = stats.LastYear;
                dataRowValues[dataColumn++] = stats.YearCount;
                dataRowValues[dataColumn++] = stats.TopYear;
                dataRowValues[dataColumn++] = stats.TopYearMatchCount;
                ngramStatsDataTable.Rows.Add(dataRowValues);

                if (ngramWordsDataTable.Rows.Count >= DatabaseSaveBatchSize || ngramStatsDataTable.Rows.Count >= DatabaseSaveBatchSize)
                    BulkWriteDataToDB(ngramWordsDataTable, statsTableName, ngramStatsDataTable, sqlBulk);
            }

            BulkWriteDataToDB(ngramWordsDataTable, statsTableName, ngramStatsDataTable, sqlBulk);
        }

        private string GetStatsTableName(int wordsCount)
            => _statsPrefix + NgramStatsTablePosfix + wordsCount;

        private string GetWordsStatsViewName(int wordsCount)
            => _statsPrefix + NgramWordsStatsViewPosfix + wordsCount;

        private void ThrowExceptionIfWriteToDatabaseTaskFailed()
        {
            if (_writeToDatabaseTask.Status == TaskStatus.Faulted)
                throw _writeToDatabaseTask.Exception;
        }

        private void CreateWordsDBTableIfNotExist(SqlConnection sqlConnection)
        {
            _log.Information($"Creating {_wordsTableName} database table if not exists");
            var createWordsTableQuery = string.Format(CreateWordsTableIfNotExistsSql, _wordsTableName, MaxWordLengthAllowed);
            var createWordsTableCommand = new SqlCommand(createWordsTableQuery, sqlConnection);

            _log.Verbose($"Executing SQL query {createWordsTableQuery}");
            createWordsTableCommand.ExecuteNonQuery();
        }

        private void CreateStatsDBTableIfNotExist(int wordsCount, SqlConnection sqlConnection)
        {
            var statsTableName = GetStatsTableName(wordsCount);
            _log.Information($"Creating {statsTableName} database table if not exists");

            var statsWordIdColumnQuery = new StringBuilder();
            var wordsTableNames = new StringBuilder();
            for (int i = 1; i <= wordsCount; ++i)
            {
                statsWordIdColumnQuery.Append(string.Format(CreateStatsWordIdColumnSql, i, statsTableName, _wordsTableName));
                wordsTableNames.Append($"Word{i}Id,");
                if (i != wordsCount) statsWordIdColumnQuery.AppendLine();
            }

            var createStatsTableQuery = string.Format(CreateStatsTableIfNotExistsSql, statsTableName, statsWordIdColumnQuery, wordsCount, wordsTableNames);
            var createStatsTableCommand = new SqlCommand(createStatsTableQuery, sqlConnection);

            _log.Verbose($"Executing SQL query {createStatsTableQuery}");
            createStatsTableCommand.ExecuteNonQuery();
        }

        private void CreateWordsStatsDBViewIfNotExist(int wordsCount, SqlConnection sqlConnection)
        {
            var wordsStatsViewName = GetWordsStatsViewName(wordsCount);
            if (WordsStatsDBViewExists(wordsStatsViewName, sqlConnection)) return;

            _log.Information($"Creating {wordsStatsViewName} database view");

            var wordsStatsViewSelectQuery = new StringBuilder();
            var wordsStatsViewJoinQuery = new StringBuilder();
            for (int i = 1; i <= wordsCount; ++i)
            {
                wordsStatsViewSelectQuery.Append(string.Format(WordsStatsViewSelectSql, i));
                wordsStatsViewJoinQuery.Append(string.Format(WordsStatsViewJoinSql, _wordsTableName, i));
                if (i != wordsCount)
                {
                    wordsStatsViewSelectQuery.AppendLine();
                    wordsStatsViewJoinQuery.AppendLine();
                }
            }

            var statsTableName = GetStatsTableName(wordsCount);
            var createWordsStatsViewQuery = string.Format(CreateWordsStatsViewSql, wordsStatsViewName, statsTableName, wordsStatsViewSelectQuery, wordsStatsViewJoinQuery);
            var createWordsStatsViewCommand = new SqlCommand(createWordsStatsViewQuery, sqlConnection);

            _log.Verbose($"Executing SQL query: {createWordsStatsViewQuery}");
            createWordsStatsViewCommand.ExecuteNonQuery();
        }

        private bool WordsStatsDBViewExists(string wordsStatsViewName, SqlConnection sqlConnection)
        {
            _log.Information($"Checking if {wordsStatsViewName} database view exists");

            var checkIfWordsStatsViewExistsQuery = string.Format(CheckIfWordsStatsViewExistsSql, wordsStatsViewName);
            var checkIfWordsStatsViewExistsCommand = new SqlCommand(checkIfWordsStatsViewExistsQuery, sqlConnection);

            _log.Verbose($"Executing SQL query: {checkIfWordsStatsViewExistsQuery}");
            return checkIfWordsStatsViewExistsCommand.ExecuteScalar() != DBNull.Value;
        }

        private long GetMaxIdFromSqlTable(string sqlTableName, SqlConnection sqlConnection)
        {
            var selectMaxIdSqlQuery = string.Format(SelectMaxIdSql, sqlTableName);
            var selectMaxIdSqlCommand = new SqlCommand(selectMaxIdSqlQuery, sqlConnection);

            _log.Verbose($"Executing SQL query: {selectMaxIdSqlQuery}");
            var maxId = selectMaxIdSqlCommand.ExecuteScalar();
            return maxId != DBNull.Value ? (long)maxId : 0;
        }

        private Dictionary<string, long> GetWordsDictionary(SqlConnection sqlConnection)
        {
            _log.Information($"Loading words from {_wordsTableName} SQL table");
            var wordToWordId = new Dictionary<string, long>();

            var selectAllWordsQuery = string.Format(SelectAllWordsSql, _wordsTableName);
            var selectAllWordsCommand = new SqlCommand(selectAllWordsQuery, sqlConnection);

            _log.Verbose($"Executing SQL query: {selectAllWordsQuery}");
            var selectAllWordsReader = selectAllWordsCommand.ExecuteReader();
            if (selectAllWordsReader.HasRows)
            {
                while (selectAllWordsReader.Read())
                {
                    var id = selectAllWordsReader.GetInt64(0);
                    var word = selectAllWordsReader.GetString(1);
                    wordToWordId.Add(word, id);
                }
            }

            selectAllWordsReader.Close();
            _log.Information($"Finished loading words from {_wordsTableName} SQL table");

            return wordToWordId;
        }

        private static DataTable CreateNgramWordsDataTable()
        {
            var ngramWords = new DataTable();
            ngramWords.Columns.Add("Id", typeof(Int64));
            ngramWords.Columns.Add("Word", typeof(String));

            return ngramWords;
        }

        private static DataTable CreateNgramStatsDataTable(int wordsCount)
        {
            var ngramStats = new DataTable();

            for (int i = 1; i <= wordsCount; ++i)
                ngramStats.Columns.Add($"Word{i}Id", typeof(int));
            ngramStats.Columns.Add("Tags", typeof(String));
            ngramStats.Columns.Add("MatchCount", typeof(Int64));
            ngramStats.Columns.Add("VolumeCount", typeof(Int64));
            ngramStats.Columns.Add("FirstYear", typeof(short));
            ngramStats.Columns.Add("LastYear", typeof(short));
            ngramStats.Columns.Add("YearCount", typeof(short));
            ngramStats.Columns.Add("TopYear", typeof(short));
            ngramStats.Columns.Add("TopYearMatchCount", typeof(Int64));

            return ngramStats;
        }

        private void BulkWriteDataToDB(DataTable ngramWordsDataTable, string statsTableName, DataTable ngramStatsDataTable, SqlBulkCopy sqlBulk)
        {
            if (ngramWordsDataTable.Rows.Count > 0)
            {
                _log.Verbose($"Bulk writing {ngramWordsDataTable.Rows.Count} data rows to {_wordsTableName} SQL table");
                sqlBulk.DestinationTableName = _wordsTableName;
                sqlBulk.WriteToServer(ngramWordsDataTable);
                ngramWordsDataTable.Clear();
            }

            if (ngramStatsDataTable.Rows.Count > 0)
            {
                _log.Verbose($"Bulk writing {ngramStatsDataTable.Rows.Count} data rows to {statsTableName} SQL table");
                sqlBulk.DestinationTableName = statsTableName;
                sqlBulk.WriteToServer(ngramStatsDataTable);
                ngramStatsDataTable.Clear();
            }
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                EndStatsProcessing();

                _statsQueue = null;
                _writeToDatabaseTask = null;
            }
        }
    }
}