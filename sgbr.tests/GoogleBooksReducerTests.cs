using NUnit.Framework;
using SGBR.Model;
using SGBR.Filters;
using System.IO;
using System.Text;
using Serilog;
using System.Collections.Generic;

namespace SGBR.Tests
{
    public class Tests
    {
        [Test]
        public void NGramLineTagsAreCorrect()
        {
            var input = "Happy_ADJ Squirrel_NOUN\t1930,1,2";
            var expectedOutput = "Happy Squirrel\tJN\t1\t2\t1930\t1930\t1\t1930\t1"; // ADJ=J NOUN=N

            var output = ProcessNgramInput(input);

            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void NgramLineSpecialTagsAreCorrect()
        {
            var input = "Happy_ADJ Squirrel_NOUN _END_\t1930,1,2";
            // there should be ' ' after second word to replace _END_ tag
            var expectedOutput = "Happy Squirrel \tJNE\t1\t2\t1930\t1930\t1\t1930\t1"; // ADJ=J NOUN=N END=E

            var output = ProcessNgramInput(input);

            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void EmptyPostfixTagsAreNotProcessed()
        {
            // tags without words need _TAG_ form (not _TAG) - only _START_ tag is valid 
            var input = "_ADJ _NOUN _START_ _END\t1930,1,2";
            // postfix tags not attached to the end of the word should be treated as words not tags
            var expectedOutput = "_ADJ _NOUN  _END\t__S_\t1\t2\t1930\t1930\t1\t1930\t1";

            var output = ProcessNgramInput(input);

            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void RealNgramLineIsCalculatedCorrectly()
        {
            var input = "Wltebsky_NOUN\t1930,1,1\t1932,1,1\t1933,1,1\t1934,2,2\t1935,5,3\t1936,1,1\t1937,1,1\t" +
                        "1939,4,4\t1941,1,1\t1942,5,3\t1948,1,1\t1951,1,1\t1959,6,2\t1960,7,6\t1961,1,1\t1962,3,2\t" +
                        "1963,3,3\t1964,2,2\t1966,1,1\t1967,4,2\t1969,4,3\t1970,5,5\t1971,1,1\t1973,1,1\t1978,2,2\t" +
                        "1980,2,2\t1985,3,3\t1986,2,2\t1987,1,1\t1988,1,1\t1991,3,2\t1995,2,2\t2000,1,1";

            // see Extras/Wltebsky_NOUN.xlsx Excel file for calculations
            var expectedOutput = "Wltebsky\tN\t79\t65\t1930\t2000\t33\t1960\t7";

            var output = ProcessNgramInput(input);

            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void ContainsTagFilterIsAppliedCorrectly()
        {
            var input = "Happy_ADJ Squirrel_NOUN _END_\t1930,1,2\n" +
                        "Mountain_NOUN Frog_NOUN _END_\t1935,1,2";

            var expectedOutput = "Mountain Frog \tNNE\t1\t2\t1935\t1935\t1\t1935\t1";

            var filters = new INgramFilter[] {
                new ContainsTagsFilter(new WordTag[] { WordTag.Adjective })
            };

            var output = ProcessNgramInput(input, filters);

            // 1 line expected = second data row (first should be filtered)
            Assert.AreEqual(1, output.Count);
            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void WordLengthFilterIsAppliedCorrectly()
        {
            var input = "Hola_ADJ World_NOUN _END_\t1930,1,2\n" +
                        "Cats_NOUN Dogs_NOUN _END_\t1935,1,2";

            var expectedOutput = "Cats Dogs \tNNE\t1\t2\t1935\t1935\t1\t1935\t1";

            var filters = new INgramFilter[] {
                new WordLengthFilter(4)
            };

            var output = ProcessNgramInput(input, filters);

            // 1 line expected = second data row (first should be filtered as 'World' word is longer than 4 characters)
            Assert.AreEqual(1, output.Count);
            Assert.AreEqual(expectedOutput, output[0]);
        }

        [Test]
        public void HasNoLettersFilterIsAppliedCorrectly()
        {
            var input = "Hello_ADJ World_NOUN\t1930,1,2\n" +
                        "2000!_+ Dogs_NOUN\t1935,1,2";

            var expectedOutput = "Hello World\tJN\t1\t2\t1930\t1930\t1\t1930\t1";

            var filters = new INgramFilter[] {
                new HasNoLettersFilter()
            };

            var output = ProcessNgramInput(input, filters);

            // 1 line expected = first data row (second should be filtered as '2000!_+' word has no letters)
            Assert.AreEqual(1, output.Count);
            Assert.AreEqual(expectedOutput, output[0]);
        }

        private List<string> ProcessNgramInput(string input, INgramFilter[] filters = null)
        {
            using var reader = new MemoryStream(Encoding.UTF8.GetBytes(input));
            using var streamReader = new StreamReader(reader);

            var ngramStringWriter = new NgramStringWriter();
            var ngramReducer = new NgramReducer(filters, 1, ngramStringWriter, Log.Logger);
            var ngramReader = new NgramReader(ngramReducer, Log.Logger);
            ngramReader.Process(streamReader);

            return ngramStringWriter.OutputLines;
        }
    }
}
