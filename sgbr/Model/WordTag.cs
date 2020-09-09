namespace SGBR.Model
{
    public enum WordTag
    {
        Noun = 'N', // time, people
        Verb = 'V', // is, was
        Adjective = 'J', // happy, beautiful, other, such
        Adverb = 'A', // not, when
        Pronoun = 'R', // it, I
        DeterminerOrArticle = 'D', // the, a
        PrepositionOrPostposition = 'P', // of, in
        Numeral = 'M',
        Conjunction = 'C', // and, or
        Particle = 'T',
        PunctuationMark = '.',
        CatchAllOther = 'X',
        StartToken = 'S',
        EndToken = 'E',
        None = '_'
    }
}
