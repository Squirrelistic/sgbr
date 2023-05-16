using SGBR.Model;
using SGBR.Utils;
using System.Linq;

namespace SGBR.Filters
{
    public class ContainsTagsFilter : INgramFilter
    {
        private readonly WordTag[] _tags;

        public ContainsTagsFilter(string tags)
        {
            _tags = WordTagUtils.StringToTags(tags);
        }

        public ContainsTagsFilter(WordTag[] tags)
        {
            _tags = tags;
        }

        public bool ShouldReject(NgramTsvStats stats)
        {
            return _tags.Any(tag => stats.ContainsTag(tag));
        }
    }
}