using SGBR.Model;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace SGBR
{
    public class NgramParser
    {
        private readonly Regex _wordTagRegex, _soloTagRegex;
        private readonly Dictionary<string, WordTag> _googleTag2Char = new Dictionary<string, WordTag>()
            {
                { "NOUN", WordTag.Noun },
                { "VERB", WordTag.Verb },
                { "ADJ",  WordTag.Adjective },
                { "ADV",  WordTag.Adverb },
                { "PRON", WordTag.Pronoun },
                { "DET",  WordTag.DeterminerOrArticle },
                { "ADP",  WordTag.PrepositionOrPostposition },
                { "NUM",  WordTag.Numeral },
                { "CONJ", WordTag.Conjunction },
                { "PRT",  WordTag.Particle },
                { ".", WordTag.PunctuationMark },
                { "X", WordTag.CatchAllOther },
                { "START", WordTag.StartToken },
                { "END",  WordTag.EndToken },
            };

        public NgramParser()
        {
            // splits 'SomeWord_NOUN' text into 'Word' and 'NOUN'
            _wordTagRegex = new Regex(@"^(.+)_([^_]+)$", RegexOptions.Singleline | RegexOptions.Compiled);

            // splits '_NOUN_' text into '' and 'NOUN'
            _soloTagRegex = new Regex(@"^()_([^_]+)_$", RegexOptions.Singleline | RegexOptions.Compiled);
        }

        public NgramTsvStats ParseLine(string line)
        {
            var fields = line.Split('\t');
            if (fields.Length < 2)
                throw new Exception($"Invalid input line: '{line}'. At least 2 tab separated values expected.");

            var ngramWithTags = fields[0];
            (var ngram, var tags) = SplitIntoNgramAndTags(ngramWithTags);
            var ngramTsvStats = new NgramTsvStats(ngram, tags);

            for (int i = 1; i < fields.Length; ++i)
            {
                var yearData = fields[i].Split(',');
                if (yearData.Length != 3)
                    throw new Exception($"Invalid year data '{fields[i]}' in input line: '{line}'. 3 comma separated values expected.");

                var year = int.Parse(yearData[0]);
                var matchCount = long.Parse(yearData[1]);
                var volumeCount = long.Parse(yearData[2]);

                ngramTsvStats.Add(year, matchCount, volumeCount);
            }

            return ngramTsvStats;
        }

        private (string[] ngram, WordTag[] tags) SplitIntoNgramAndTags(string ngramWithTags)
        {
            var words = ngramWithTags.Split(' ');
            WordTag[] tags = new WordTag[words.Length];

            bool containsTags = false;
            for (int i = 0; i < words.Length; ++i)
            {
                var tagMatch = _wordTagRegex.Match(words[i]);
                if (!tagMatch.Success)
                    tagMatch = _soloTagRegex.Match(words[i]);

                if (tagMatch.Success && _googleTag2Char.ContainsKey(tagMatch.Groups[2].Value))
                {
                    words[i] = tagMatch.Groups[1].Value;
                    tags[i] = _googleTag2Char[tagMatch.Groups[2].Value];
                    containsTags = true;
                }
                else
                {
                    tags[i] = WordTag.None;
                }
            }

            return (words, containsTags ? tags : null);
        }
    }
}
