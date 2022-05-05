// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using ElasticLinq.Async;
using ElasticLinq.Logging;
using ElasticLinq.Mapping;
using ElasticLinq.Request;
using ElasticLinq.Request.Criteria;
using ElasticLinq.Request.Formatters;
using ElasticLinq.Request.Helpers;
using ElasticLinq.Request.Visitors;
using ElasticLinq.Response.Model;
using ElasticLinq.Retry;
using ElasticLinq.Utility;
using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace ElasticLinq
{
    /// <summary>
    /// Query provider implementation for Elasticsearch.
    /// </summary>
    public sealed class ElasticQueryProvider : IQueryProvider, IAsyncQueryExecutor
    {
       
        private readonly IElasticClient _elasticClient;
        /// <summary>
        /// Create a new ElasticQueryProvider for a given connection, mapping, log, retry policy and field prefix.
        /// </summary>
        /// <param name="elasticClient">The NEST Elastic Client used to connect to ElasticSearch).</param>
        /// <param name="mapping">The object that helps map queries (optional, defaults to <see cref="TrivialElasticMapping"/>).</param>
        /// <param name="log">The object which logs information (optional, defaults to <see cref="NullLog"/>).</param>
        public ElasticQueryProvider(IElasticMapping mapping, IElasticClient elasticClient, ILog log)
        {
            Argument.EnsureNotNull(nameof(mapping), mapping);
            Argument.EnsureNotNull(nameof(elasticClient), elasticClient);

            _elasticClient = elasticClient;
            Log = log;
            Mapping = mapping;
            

            //requestProcessor = new ElasticRequestProcessor(connection, mapping, log, retryPolicy);
        }
        /// <summary>
        /// The mapping to describe how objects and their properties are mapped to Elasticsearch.
        /// </summary>
        internal IElasticMapping Mapping { get; }
        /// <summary>
        /// The logging mechanism for diagnostic information.
        /// </summary>
        public ILog Log { get; }

       

        /// <inheritdoc/>
        public IQueryable<T> CreateQuery<T>(Expression expression)
        {
            Argument.EnsureNotNull(nameof(expression), expression);

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException(nameof(expression));

            return new ElasticQuery<T>(this, expression);
        }

        /// <inheritdoc/>
        public IQueryable CreateQuery(Expression expression)
        {
            Argument.EnsureNotNull(nameof(expression), expression);

            var elementType = TypeHelper.GetSequenceElementType(expression.Type);
            var queryType = typeof(ElasticQuery<>).MakeGenericType(elementType);
            try
            {
                return (IQueryable)Activator.CreateInstance(queryType, this, expression);
            }
            catch (TargetInvocationException ex)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                return null;  // Never called, as the above code re-throws
            }
        }

        /// <inheritdoc/>
        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }

        /// <inheritdoc/>
        public object Execute(Expression expression)
        {
            return AsyncHelper.RunSync(() => ExecuteAsync(expression));
        }

        /// <inheritdoc/>
        public async Task<TResult> ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken = default(CancellationToken))
        {
            return (TResult)await ExecuteAsync(expression, cancellationToken).ConfigureAwait(false);
        }

        internal static ElasticResponse ParseResponse(SearchResponse<object> response)
        {
            

            var r = JsonConvert.SerializeObject(response);
            var hits = response.HitsMetadata;
            var hitsR = JsonConvert.SerializeObject(hits);

            var hitsDer = JsonConvert.DeserializeObject<Hits>(hitsR);

            ElasticResponse er = new ElasticResponse();
            er.took = response.Took;
            er.timed_out = response.TimedOut;
            er.hits = hitsDer;

            return er;
            
        }

    internal static IEnumerable<string> GetResultSummary(ElasticResponse results)
    {
        if (results == null)
        {
            yield return "nothing";
        }
        else
        {
            if (results.hits?.hits != null && results.hits.hits.Length > 0)
                yield return results.hits.hits.Length + " hits";
        }
    }

        public string TryGetIndex(Type type)
        {
            var keyName = (_elasticClient.ConnectionSettings.DefaultIndices.ContainsKey(type) ? _elasticClient.ConnectionSettings.DefaultIndices[type] : _elasticClient.ConnectionSettings.DefaultIndex);
            return keyName;
        }

    /// <inheritdoc/>
    public async Task<object> ExecuteAsync(Expression expression, CancellationToken cancellationToken = default(CancellationToken))
        {
            Argument.EnsureNotNull(nameof(expression), expression);

            var translation = ElasticQueryTranslator.Translate(Mapping, expression);
            await Task.Delay(1);

            try
            {
                ElasticResponse response;
                bool v = true;
                if (!v)
                {
                    response = new ElasticResponse();
                }
                else
                {

                    Type elementType = TypeSystem.GetElementType(expression.Type);
                    Type expType = TypeSystem.GetElementType(expression.Type);

                    if (Attribute.IsDefined(expType, typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), false)
                        && expType.IsGenericType && expType.Name.Contains("AnonymousType")
                        && (expType.Name.StartsWith("<>") || expType.Name.StartsWith("VB$"))
                        && (expType.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic)
                    {
                        var mExp = expression as MethodCallExpression;
                        var t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type);

                        while (t1.IsGenericType)
                        {
                            mExp = mExp.Arguments[0] as MethodCallExpression;
                            t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type);
                        }

                        elementType = t1;
                    }

                   // Whem use IQueryable.Sum() for example need make this to get the elementType
                    if (!elementType.IsClass)
                        {
                            var exp = expression as MethodCallExpression;
                            elementType = exp.Arguments[0].Type.GenericTypeArguments[0];
                        }
                  

                    var formatter = new SearchRequestFormatter(Mapping, translation.SearchRequest);
                    formatter.Fill();

                    var indexName = TryGetIndex(elementType);
                    var t = _elasticClient.LowLevel.Search<SearchResponse<object>>(indexName, formatter.FullQuery.ToString());
                  
                    
                    dynamic request = t;
                    var sr = ParseResponse(t);
                    


                    var resultGood = translation.Materializer.Materialize(sr);
                  
                    return resultGood;


                }

                return response;
      
            }
            catch (AggregateException ex)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                return null;  // Never called, as the above code re-throws
            }
        }

      

       


    }
}
