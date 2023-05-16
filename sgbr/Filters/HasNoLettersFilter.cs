using SGBR.Model;
using System.Globalization;
using System.Linq;

namespace SGBR.Filters
{
    public class HasNoLettersFilter : INgramFilter
    {
        public bool ShouldReject(NgramTsvStats stats)
        {
            return stats.Ngram.Any(word => word.Length > 0 && HasNoLetters(word));
        }

        private bool HasNoLetters(string word)
        {
            return !word.Any(letter => IsUnicodeLetter(letter));
        }

        private bool IsUnicodeLetter(char letter)
        {
            var unicodeCategory = CharUnicodeInfo.GetUnicodeCategory(letter);

            return unicodeCategory == UnicodeCategory.UppercaseLetter ||
                   unicodeCategory == UnicodeCategory.LowercaseLetter ||
                   unicodeCategory == UnicodeCategory.TitlecaseLetter ||
                   unicodeCategory == UnicodeCategory.ModifierLetter ||
                   unicodeCategory == UnicodeCategory.OtherLetter;
        }
    }
}