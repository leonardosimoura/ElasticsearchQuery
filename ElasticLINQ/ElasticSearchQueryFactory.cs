using Nest;
using System;
using System.Linq;
using System.Linq.Expressions;
using ElasticLinq.Mapping;
using ElasticLinq.Logging;

namespace ElasticLinq
{
    public class ElasticSearchQueryFactory
    {
        [Obsolete("This method will be removed in version 1.0. Use the ElasticSearchQueryFactory.CreateQuery.")]
        public static IQueryable<TEntity> GetQuery<TEntity>(IElasticClient client, IElasticMapping mapping, ILog log)
        {
            var provider = new ElasticQueryProvider(mapping, client, log);
            return new ElasticQuery<TEntity>(provider);
        }

        public static IQueryable<TEntity> CreateQuery<TEntity>(IElasticClient client, IElasticMapping mapping, ILog log,  Expression expression = null)
        {
            var provider = new ElasticQueryProvider(mapping,client, log);
            if (expression != null)
            {
                return new ElasticQuery<TEntity>(provider, expression);
            }
            return new ElasticQuery<TEntity>(provider);
        }
    }
}
