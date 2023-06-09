using ElasticsearchQuery.NameProviders;
using Nest;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace ElasticsearchQuery
{
    public class ElasticsearchQueryFactory
    {
        [Obsolete("This method will be removed in version 1.0. Use the ElasticsearchQueryFactory.CreateQuery.")]
        public static IQueryable<TEntity> GetQuery<TEntity>(IElasticClient client, IProvideIndexName indexNameProvider)
        {
            var indexName = indexNameProvider.GetIndexName(typeof(TEntity));
            var provider = new ElasticQueryProvider(client, indexName);
            return new ElasticQuery<TEntity>(provider);
        }

        public static IQueryable<TEntity> CreateQuery<TEntity>(IElasticClient client, IProvideIndexName indexNameProvider)
        {
            var indexName = indexNameProvider.GetIndexName(typeof(TEntity));
            var provider = new ElasticQueryProvider(client, indexName);
            return new ElasticQuery<TEntity>(provider);
        }

        public static IQueryable<TEntity> CreateQuery<TEntity>(IElasticClient client, Expression exp, IProvideIndexName indexNameProvider)
        {
            var indexName = indexNameProvider.GetIndexName(typeof(TEntity));
            var provider = new ElasticQueryProvider(client, indexName);
            return new ElasticQuery<TEntity>(provider, exp);
        }
    }
}
