// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json.Serialization;

namespace ElasticLinq.Response.Model
{
    /// <summary>
    /// An individual hit response from Elasticsearch.
    /// </summary>
    
    public class Hit
    {
        /// <summary>
        /// The index of the document responsible for this hit.
        /// </summary>
        [JsonProperty("Index")]
        public string _index;

        /// <summary>
        /// The type of document used to create this hit.
        /// </summary>
        [JsonProperty("Type")]
        public string _type;

        /// <summary>
        /// Unique index of the document responsible for this hit.
        /// </summary>
        [JsonProperty("Id")]
        public string _id;

        /// <summary>
        /// The score this hit achieved based on the query criteria.
        /// </summary>
        [JsonProperty("Score")]
        public double? _score;

        /// <summary>
        /// Highlighting for this hit if highlighting was requested.
        /// </summary>
        [JsonProperty("Highlight")]
        public JObject highlight;

        /// <summary>
        /// The actual document for this hit (not supplied if fields requested).
        /// </summary>
        [JsonProperty("Source")]
        public JObject _source;
    }
}