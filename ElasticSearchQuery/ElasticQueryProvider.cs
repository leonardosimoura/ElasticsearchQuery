using Elasticsearch.Net;
using ElasticsearchQuery.Helpers;
using ElasticSearchQuery;
using Nest;
using System;
using System.Drawing;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Expressions;
using System.Reflection;

namespace ElasticsearchQuery
{
    internal class ElasticQueryProvider : QueryProvider
    {
        private readonly IElasticClient _elasticClient;

        private Type _elementType;

        private Type _expType;

        private QueryTranslateResult _nestQuery;

        private MethodInfo _search;

        private Type _searchResponse;

        private int _maxResultWindow = 10000;

        private bool IsAnAggregationQuery => _nestQuery.Aggregation.Any() || _nestQuery.ReturnNumberOfRows;

        private bool IsQueryWithinMaxResultWindow => _nestQuery.SearchRequest.From is null || _nestQuery.SearchRequest.From <= (_maxResultWindow - _nestQuery.SearchRequest.Size);

        private bool IsQueryBeyondMaxResultWindow => _nestQuery.SearchRequest.From > (_maxResultWindow - _nestQuery.SearchRequest.Size);

        public ElasticQueryProvider(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }
        public override object Execute(Expression expression)
        {
            //Element type of IQueryable
            //Need this for the elastic search request
            _elementType = TypeSystem.GetElementType(expression.Type);
            _expType = TypeSystem.GetElementType(expression.Type);

            if (Attribute.IsDefined(_expType, typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), false)
                && _expType.IsGenericType && _expType.Name.Contains("AnonymousType")
                && (_expType.Name.StartsWith("<>") || _expType.Name.StartsWith("VB$"))
                && (_expType.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic)
            {
                var mExp = expression as MethodCallExpression;
                var t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type);

                while (t1.IsGenericType)
                {
                    mExp = mExp.Arguments[0] as MethodCallExpression;
                    t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type);
                }

                _elementType = t1;
            }

            //Whem use IQueryable.Sum() for example need make this to get the _elementType
            if (!_elementType.IsClass)
            {
                var exp = expression as MethodCallExpression;
                _elementType = exp.Arguments[0].Type.GenericTypeArguments[0];
            }

            var indexName = ElasticQueryMapper.GetMap(_elementType).Index;
            _nestQuery = new QueryTranslator().Translate(expression, indexName);
            _searchResponse = typeof(SearchResponse<>).MakeGenericType(_elementType);

            var searchMethod = typeof(ElasticClient)
                          .GetMethods()
                          .Where(m => m.Name == "Search")
                          .Select(m => new
                          {
                              Method = m,
                              Params = m.GetParameters(),
                              Args = m.GetGenericArguments()
                          })
                        .Where(x => x.Params.Length == 1
                                    && x.Args.Length == 1
                                    && x.Params.First().ParameterType == typeof(ISearchRequest))
                        .Select(x => x.Method).First();

            _search = searchMethod.MakeGenericMethod(_elementType);

            return Execute(indexName);
        }

        protected virtual object Execute(string indexName)
        {
            var settings = _elasticClient.Indices.GetSettings(indexName);
            if(!settings.IsValid)
            {
                new ElasticResponseException(settings, _elementType);
            }
            var indexKey = settings.Indices.Keys.FirstOrDefault();
            if(settings.Indices.TryGetValue(indexKey ?? indexName, out IndexState value))
            {
                if(value.Settings.TryGetValue("index.max_result_window", out object MaxResultWindow))
                {
                    _maxResultWindow = Convert.ToInt32(MaxResultWindow);
                }
            }
            if (IsAnAggregationQuery)
            {
                return ExecuteForAggregations();
            }
            else if (IsQueryWithinMaxResultWindow)
            {
                return ExecuteWithinMaxResultWindow();
            }
            else if (IsQueryBeyondMaxResultWindow)
            {
                return ExecuteBeyondMaxResultWindow();
            }
            return new { };
        }

        private object ExecuteForAggregations()
        {
            _nestQuery.SearchRequest.Size = 1;
            _nestQuery.SearchRequest.Scroll = "3s";

            dynamic response = _search.Invoke(_elasticClient, new object[] { _nestQuery.SearchRequest });

            dynamic clearScrollResponse = _elasticClient.ClearScroll(cs => cs.ScrollId(response.ScrollId));

            if (!response.IsValid)
            {
                new ElasticResponseException(response, _elementType);
            }
            if (_nestQuery.ReturnNumberOfRows)
            {
                return Convert.ChangeType(response.HitsMetadata.Total.Value, _expType);
            }
            else
            {
                return DynamicTypeBuilder.ToList((AggregateDictionary)response.Aggregations, Convert.ToInt32(response.HitsMetadata.Total.Value));
            }
        }

        private object ExecuteWithinMaxResultWindow()
        {
            dynamic response = _search.Invoke(_elasticClient, new object[] { _nestQuery.SearchRequest });

            if (!response.IsValid)
            { 
                new ElasticResponseException(response, _elementType);
            }
            return ExecuteFromSearchResponse(response);
        }

        private object ExecuteFromSearchResponse(dynamic response)
        {
            var prop = _searchResponse.GetProperty("Documents");

            if (prop.PropertyType.GetGenericArguments().Count() == 1 &&
                prop.PropertyType.GetGenericArguments().First() == _expType)
            {
                var value = prop.GetValue(response);
                return value;
            }
            return new { };
        }

        private object ExecuteBeyondMaxResultWindow()
        {
            var originalRequestSize = _nestQuery.SearchRequest.Size;
            var iterator = _nestQuery.SearchRequest.From;
            _nestQuery.SearchRequest.From = 0;
            while (iterator > _maxResultWindow - originalRequestSize)
            {
                Execute(_maxResultWindow - originalRequestSize, true);
                iterator -= _maxResultWindow - originalRequestSize;
            }
            if(iterator > 0)
            {
                Execute(iterator, true);
            }
            return ExecuteFromSearchResponse(Execute(originalRequestSize, false));
        }

        private object Execute(int? size, bool isLightWeight)
        {
            _nestQuery.SearchRequest.Size = size ?? 1;
            _nestQuery.SearchRequest.Fields = isLightWeight ? "uniqueId" : null;
            _nestQuery.SearchRequest.Source = !(isLightWeight);

            dynamic response = _search.Invoke(_elasticClient, new object[] { _nestQuery.SearchRequest });
            if (!response.IsValid)
            {
                new ElasticResponseException(response, _elementType);
            }
            _nestQuery.SearchRequest.SearchAfter = response.Hits[response.Hits.Length - 1].Sorts;
            return response;
        }
    }
}
