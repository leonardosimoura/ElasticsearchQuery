using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ElasticsearchQuery.Extensions
{
    public static class ObjectExtensions
    {
        public static bool MultiMatch<TObj>(this TObj obj, string query, params Expression<Func<TObj,object>>[] fields)
        {
            return true;
        }

        public static bool Exists<TObj>(this TObj obj, Expression<Func<TObj, object>> field)
        {
            return true;
        }
    }
}
