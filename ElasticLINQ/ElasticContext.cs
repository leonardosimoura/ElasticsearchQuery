// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using ElasticLinq.Logging;
using ElasticLinq.Mapping;
using ElasticLinq.Retry;
using ElasticLinq.Utility;
using Nest;
using System.Linq;

namespace ElasticLinq
{
    /// <summary>
    /// Provides an entry point to easily create LINQ queries for Elasticsearch.
    /// </summary>
    public class ElasticContext : IElasticContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ElasticContext"/> class.
        /// </summary>
        
        /// <param name="mapping">The object that helps map queries (optional, defaults to <see cref="TrivialElasticMapping"/>).</param>
        /// <param name="client">The NEST Elastic Client</param>
        /// <param name="log">The logging mechanism</param>
        public ElasticContext(IElasticClient client, ILog log, IElasticMapping mapping = null)
        {

            Log = log;
          
            Mapping = mapping ?? new TrivialElasticMapping();
            Client = client;
        }

        /// <summary>
        /// Specifies the connection to the Elasticsearch server.
        /// </summary>
        public IElasticConnection Connection { get; }

        /// <summary>
        /// The NEST Elsatic Client
        /// </summary>
        public IElasticClient Client { get; }

        /// <summary>
        /// The logging mechanism for diagnostic information.
        /// </summary>
        public ILog Log { get; }

        /// <summary>
        /// The mapping to describe how objects and their properties are mapped to Elasticsearch.
        /// </summary>
        public IElasticMapping Mapping { get; }

        /// <summary>
        /// The retry policy for handling networking issues.
        /// </summary>
        public IRetryPolicy RetryPolicy { get; }

        /// <inheritdoc/>
        public virtual IQueryable<T> Query<T>()
        {
            return new ElasticQuery<T>(new ElasticQueryProvider(Mapping, Client, Log));
        }
    }
}