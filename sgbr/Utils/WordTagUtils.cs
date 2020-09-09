using SGBR.Model;
using System.Text;

namespace SGBR.Utils
{
    public class WordTagUtils
    {
        public static string TagsToString(WordTag[] tags)
        {
            if (tags == null)
                return string.Empty;

            var result = new StringBuilder(tags.Length);
            foreach (var tag in tags)
                result.Append((char)tag);

            return result.ToString();
        }

        public static WordTag[] StringToTags(string str)
        {
            if (string.IsNullOrEmpty(str))
                return null;

            var result = new WordTag[str.Length];
            for (int i = 0; i < str.Length; ++i)
                result[i] = (WordTag)str[i];

            return result;
        }
    }
}
