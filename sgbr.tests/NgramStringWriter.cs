using SGBR.Model;
using System.Collections.Generic;

namespace SGBR.Tests
{
    public class NgramStringWriter : INgramStatsProcessor
    {
        public List<string> OutputLines { get; private set; } = new List<string>();

        public void InitStatsProcessing() { }

        public void ProcessStats(NgramTsvStats stats) => OutputLines.Add(stats.ToTsvLine());

        public void EndStatsProcessing() { }
    }
}
