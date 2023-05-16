using SGBR.Model;

namespace SGBR.Filters
{
    public interface INgramFilter
    {
        // must be multi-thread safe
        public bool ShouldReject(NgramTsvStats stats);
    }
}