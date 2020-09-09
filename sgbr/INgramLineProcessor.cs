namespace SGBR
{
    public interface INgramLineProcessor
    {
        public void InitLineProcessing();
        public void ProcessLine(string line);
        public void EndLineProcessing();
    }
}
