using System;
using System.Collections.Generic;
using System.Text;

namespace ElasticsearchQuery.Extensions
{
    public static class StringExtensions
    {
        public static string ToCamelCase(this string str)
        {
            if (string.IsNullOrWhiteSpace(str))
                return str;

            return str.Trim()[0].ToString().ToLower() + str.Trim().Substring(1);
        }
    }
}
