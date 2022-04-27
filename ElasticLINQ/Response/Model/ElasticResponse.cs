// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using System.Text.Json.Serialization;

namespace ElasticLinq.Response.Model
{
    /// <summary>
    /// A top-level response from Elasticsearch.
    /// </summary>
    [DebuggerDisplay("{hits.hits.Count} hits in {took} ms")]
    public class ElasticResponse
    {

        /// <summary>
        /// How long the request took in milliseconds.
        /// </summary>
        [JsonProperty("Took")]
        public long took;

        /// <summary>
        /// Whether this request timed out or not.
        /// </summary>
        [JsonProperty("TimedOut")]
        public bool timed_out;

        /// <summary>
        /// The search hits delivered in this response.
        /// </summary>
        [JsonProperty("HitsMetadata")]
        public Hits hits;

        /// <summary>
        /// The error received from Elasticsearch.
        /// </summary>
        [JsonProperty("Error")]
        public JValue error;
    }
}