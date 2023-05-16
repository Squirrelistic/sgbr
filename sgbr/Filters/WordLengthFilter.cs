using SGBR.Model;
using System.Linq;

namespace SGBR.Filters
{
    public class WordLengthFilter : INgramFilter
    {
        private readonly int _maxLength;

        // 80 = length of German Word "Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft"
        public WordLengthFilter(int maxLength = 80)
        {
            _maxLength = maxLength;
        }

        public bool ShouldReject(NgramTsvStats stats)
        {
            return stats.Ngram.Any(word => word.Length > _maxLength);
        }
    }
}