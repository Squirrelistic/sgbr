using Serilog;
using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace SGBR
{
    public class NgramReader
    {
        private readonly INgramLineProcessor _ngramProcessor;
        private readonly ILogger _log;

        public NgramReader(INgramLineProcessor ngramProcessor, ILogger log)
        {
            _ngramProcessor = ngramProcessor;
            _log = log;
        }

        public void ProcessGzipFile(string inputFileName)
        {
            _log.Information($"Processing file {inputFileName}");

            using var inputFileStream = new FileStream(inputFileName, FileMode.Open, FileAccess.Read);
            ProcessGzipStream(inputFileStream);
        }

        public async Task ProcessGzipUrl(string inputUrl)
        {
            _log.Information($"Processing URL {inputUrl}");
            var httpClient = new HttpClient();

            // without HttpCompletionOption.ResponseHeadersRead the whole file is downloaded to memory first
            var response = await httpClient.GetAsync(inputUrl, HttpCompletionOption.ResponseHeadersRead);
            response.EnsureSuccessStatusCode();

            using var inputHttpStream = await response.Content.ReadAsStreamAsync();
            ProcessGzipStream(inputHttpStream);
        }

        public void ProcessGzipDir(string inputDir)
        {
            _log.Information($"Processing directory {inputDir}");

            var gzipFiles = Directory.GetFiles(inputDir, "*.gz");
            if (gzipFiles.Length == 0)
                throw new Exception($"No *.gz files found in {inputDir} directory");

            foreach (var gzipFile in gzipFiles)
                ProcessGzipFile(Path.Combine(inputDir, gzipFile));
        }

        private void ProcessGzipStream(Stream inputStream)
        {
            using var inputGzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            using var inputStreamReader = new StreamReader(inputGzipStream, new UTF8Encoding(false));

            Process(inputStreamReader);
        }

        public void Process(StreamReader inputStreamReader)
        {
            _ngramProcessor.InitLineProcessing();

            string line;
            while ((line = inputStreamReader.ReadLine()) != null)
                _ngramProcessor.ProcessLine(line);

            _ngramProcessor.EndLineProcessing();
        }
    }
}
