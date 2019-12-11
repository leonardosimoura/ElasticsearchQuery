using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace ElasticsearchQuery.Extensions
{
    public static class IQueryableExtensions
    {
        public static IQueryable<TEntity> SimpleQuery<TEntity>(this IQueryable<TEntity>  queryable,Expression<Func<TEntity,object>> exp , string query)
        {
            var _elasticQuery = queryable as ElasticQuery<TEntity>;
            _elasticQuery.SimpleQuery(exp, query);
            return queryable;
        }
    }
}
