using Serilog;
using SGBR.Model;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;

namespace SGBR
{
    public class NgramFileWriter : INgramStatsProcessor, IDisposable
    {
        private const int StatsQueueBufferSize = 2131;

        private readonly string _outputFileName;
        private readonly ILogger _log;

        private BlockingCollection<NgramTsvStats> _statsQueue;
        private Task _writeToFileTask;
        private bool _isDisposed;

        public NgramFileWriter(string outputFileName, ILogger log)
        {
            _outputFileName = outputFileName;
            _log = log;
            InitStatsProcessing();
        }

        public void InitStatsProcessing()
        {
            _statsQueue = new BlockingCollection<NgramTsvStats>(StatsQueueBufferSize);

            // writing compressed stream in separate task results in ~15% speed improvement
            _writeToFileTask = Task.Run(() => WriteToFileTask());
        }

        public void ProcessStats(NgramTsvStats stats)
        {
            if (_isDisposed) throw new Exception($"{GetType().Name} object already disposed");

            do ThrowExceptionIfWriteToFileTaskFailed();
            while (!_statsQueue.TryAdd(stats, 1000));
        }

        public void EndStatsProcessing()
        {
            _statsQueue.CompleteAdding();
            _writeToFileTask.Wait();
            ThrowExceptionIfWriteToFileTaskFailed();
        }

        private void WriteToFileTask()
        {
            var tempOutputFileName = _outputFileName + ".tmp";

            try
            {
                _log.Information($"Creating temporary output file {tempOutputFileName}");
                using var outputFileStream = new FileStream(tempOutputFileName, FileMode.Create, FileAccess.Write);
                using var outputGzipStream = new GZipStream(outputFileStream, CompressionLevel.Optimal);
                using var outputStreamWriter = new StreamWriter(outputGzipStream, new UTF8Encoding(false))
                {
                    NewLine = "\n" // Unix style
                };

                outputStreamWriter.WriteLine(NgramTsvStats.TsvHeader);

                foreach (var stats in _statsQueue.GetConsumingEnumerable())
                    outputStreamWriter.WriteLine(stats.ToTsvLine());

                outputStreamWriter.Close();
                outputGzipStream.Close();
                outputStreamWriter.Close();

                _log.Information($"Moving output file from {tempOutputFileName} to {_outputFileName}");
                File.Move(tempOutputFileName, _outputFileName, true);
            }
            finally
            {
                if (File.Exists(tempOutputFileName))
                    File.Delete(tempOutputFileName);
            }
        }

        private void ThrowExceptionIfWriteToFileTaskFailed()
        {
            if (_writeToFileTask.Status == TaskStatus.Faulted)
            {
                _log.Error("Exception detected in WriteToFile task");
                throw _writeToFileTask.Exception;
            }
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                EndStatsProcessing();

                _statsQueue = null;
                _writeToFileTask = null;
            }
        }
    }
}