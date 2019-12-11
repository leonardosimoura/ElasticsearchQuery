using System;

namespace ElasticsearchQuery.QueryExtensions
{
    public static class StringExtensions
    {
        public static bool MatchPhrase(this string str,string exp)
        {
            return true;
        }
    }
}
