using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ElasticsearchQuery.QueryExtensions
{
    public static class ObjectExtensions
    {
        public static bool MultiMatch<TObj>(this TObj obj, string query, params Expression<Func<TObj,object>>[] fields)
        {
            return true;
        }
    }
}
