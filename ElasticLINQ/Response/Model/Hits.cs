// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using Newtonsoft.Json;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json.Serialization;

namespace ElasticLinq.Response.Model
{
    /// <summary>
    /// A container of hit responses from Elasticsearch.
    /// </summary>
   
    public class Hits
    {
        /// <summary>
        /// The total number of hits available on the server.
        /// </summary>
        [JsonProperty("Total")]
        public Total total;

        /// <summary>
        /// The highest score of a hit for the given query.
        /// </summary>
        [JsonProperty("MaxScore")]
        public double? max_score;

        /// <summary>
        /// The list of hits received from the server.
        /// </summary>
        [JsonProperty("Hits")]
        public Hit[] hits;
    }
}