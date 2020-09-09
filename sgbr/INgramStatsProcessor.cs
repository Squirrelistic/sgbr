using SGBR.Model;

namespace SGBR
{
    public interface INgramStatsProcessor
    {
        public void ProcessStats(NgramTsvStats stats);
    }
}
