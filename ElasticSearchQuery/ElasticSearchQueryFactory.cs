using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticSearchQuery
{
    public class ElasticSearchQueryFactory
    {

        public static IQueryable<TEntity> GetQuery<TEntity>(IElasticClient client)
        {
            var provider = new ElasticQueryProvider(client);
            return new ElasticQuery<TEntity>(provider);
        }
    }
}
