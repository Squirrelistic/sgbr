using Serilog;
using SGBR.Filters;
using SGBR.Model;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SGBR
{
    public class NgramReducer : INgramLineProcessor
    {
        private const int VerboseMessageEveryInputLine = 100000;
        private const int LinesQueueBufferSize = 4913;

        private readonly IEnumerable<INgramFilter> _filters;
        private readonly int _processingThreads;
        private readonly INgramStatsProcessor _statsProcessor;
        private readonly ILogger _log;

        private BlockingCollection<string> _linesQueue;
        private long _linesRead;
        private Task<long>[] _processingTasks;
        private bool _isFileInReducedFormat;

        public NgramReducer(IEnumerable<INgramFilter> filters, byte processingThreads, INgramStatsProcessor statsProcessor, ILogger log)
        {
            _filters = filters;
            _processingThreads = processingThreads;
            _statsProcessor = statsProcessor;
            _log = log;
        }

        public void InitLineProcessing()
        {
            _linesQueue = new BlockingCollection<string>(LinesQueueBufferSize);
            _linesRead = 0;

            // using threads for processing results in up to ~100% speed improvement
            _log.Verbose($"Creating {_processingThreads} line processing threads");
            _processingTasks = new Task<long>[_processingThreads];
            for (int i = 0; i < _processingThreads; ++i)
                _processingTasks[i] = Task.Run(() => ProcessLineTask());
        }

        public void ProcessLine(string line)
        {
            if (_linesRead++ == 0)
            {
                _isFileInReducedFormat = line == NgramTsvStats.TsvHeader;
                if (_isFileInReducedFormat) // skip header
                    return;
            }

            // throw exception if any task fails, otherwise the queues will fill up, block, and cause deadlock
            do ThrowExceptionIfProcessingTaskFailed();
            while (!_linesQueue.TryAdd(line, 1000));

            if (_linesRead % VerboseMessageEveryInputLine == 0)
                _log.Verbose($"{_linesRead} lines read");
        }

        public void EndLineProcessing()
        {
            _linesQueue.CompleteAdding();
            Task.WaitAll(_processingTasks);
            ThrowExceptionIfProcessingTaskFailed();

            long linesFiltered = _processingTasks.Sum(x => x.Result);
            _log.Information($"Processing finished: lines processed={_linesRead}, filtered={linesFiltered}");

            _linesQueue = null;
            _processingTasks = null;
        }

        // Multi-threaded task
        private long ProcessLineTask()
        {
            var ngramParser = new NgramParser();
            int linesFiltered = 0;
            foreach (var line in _linesQueue.GetConsumingEnumerable())
            {
                var ngramTsvStats = _isFileInReducedFormat ? NgramTsvStats.FromTsvLine(line) : ngramParser.ParseLine(line);
                if (_filters != null && _filters.Any(x => x.ShouldReject(ngramTsvStats)))
                    linesFiltered++;
                else
                    _statsProcessor.ProcessStats(ngramTsvStats);
            }

            return linesFiltered;
        }

        private void ThrowExceptionIfProcessingTaskFailed()
        {
            var processingException = _processingTasks.FirstOrDefault(x => x.Status == TaskStatus.Faulted);
            if (processingException != null)
            {
                _log.Error("Exception detected in ProcessLine task");
                throw processingException.Exception;
            }
        }
    }
}