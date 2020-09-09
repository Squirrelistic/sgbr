using SGBR.Utils;
using System;
using System.Linq;

namespace SGBR.Model
{
    // Tsv = Tab Separated Values
    public class NgramTsvStats
    {
        public const string DataSeparator = "\t";
        public const string NgramSeparator = " ";

        public const string TsvHeader =
            "Ngram" + DataSeparator +
            "Tags" + DataSeparator +
            "MatchCount" + DataSeparator +
            "VolumeCount" + DataSeparator +
            "FirstYear" + DataSeparator +
            "LastYear" + DataSeparator +
            "YearCount" + DataSeparator +
            "TopYear" + DataSeparator +
            "TopYearMatchCount";

        public string[] Ngram { get; private set; }
        public WordTag[] Tags { get; private set; }
        public long MatchCount { get; set; }
        public long VolumeCount { get; set; }
        public int FirstYear { get; set; }
        public int LastYear { get; set; }
        public int YearCount { get; set; }
        public int TopYear { get; set; }
        public long TopYearMatchCount { get; set; }

        public NgramTsvStats(string[] ngram, WordTag[] tags)
        {
            Ngram = ngram;
            Tags = tags;
        }

        public void Add(int year, long matchCount, long volumeCount)
        {
            MatchCount += matchCount;
            VolumeCount += volumeCount;

            FirstYear = FirstYear == 0 ? year : Math.Min(FirstYear, year);
            LastYear = Math.Max(LastYear, year);

            YearCount++;

            if (matchCount > TopYearMatchCount)
            {
                TopYear = year;
                TopYearMatchCount = matchCount;
            }
        }

        public bool ContainsTag(WordTag wordTag)
        {
            return Tags != null && Tags.Contains(wordTag);
        }

        public static NgramTsvStats FromTsvLine(string line)
        {
            var splitLine = line.Split(DataSeparator);
            return new NgramTsvStats(splitLine[0].Split(NgramSeparator), WordTagUtils.StringToTags(splitLine[1]))
            {
                MatchCount = long.Parse(splitLine[2]),
                VolumeCount = long.Parse(splitLine[3]),
                FirstYear = int.Parse(splitLine[4]),
                LastYear = int.Parse(splitLine[5]),
                YearCount = int.Parse(splitLine[6]),
                TopYear = int.Parse(splitLine[7]),
                TopYearMatchCount = long.Parse(splitLine[8]),
            };
        }

        public string ToTsvLine()
        {
            return string.Join(NgramSeparator, Ngram) + DataSeparator +
                   WordTagUtils.TagsToString(Tags) + DataSeparator +
                   MatchCount + DataSeparator +
                   VolumeCount + DataSeparator +
                   FirstYear + DataSeparator +
                   LastYear + DataSeparator +
                   YearCount + DataSeparator +
                   TopYear + DataSeparator +
                   TopYearMatchCount;
        }
    }
}
