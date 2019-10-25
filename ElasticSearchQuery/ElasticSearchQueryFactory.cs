using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticsearchQuery
{
    public class ElasticSearchQueryFactory
    {
        [Obsolete("This method will be removed in version 1.0. Use the ElasticSearchQueryFactory.CreateQuery.")]
        public static IQueryable<TEntity> GetQuery<TEntity>(IElasticClient client)
        {
            var provider = new ElasticQueryProvider(client);
            return new ElasticQuery<TEntity>(provider);
        }

        public static IQueryable<TEntity> CreateQuery<TEntity>(IElasticClient client)
        {
            var provider = new ElasticQueryProvider(client);
            return new ElasticQuery<TEntity>(provider);
        }
    }
}
