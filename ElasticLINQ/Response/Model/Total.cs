// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using Newtonsoft.Json;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json.Serialization;

namespace ElasticLinq.Response.Model
{

    
    public class Total
    {
       
        [JsonProperty("Relation")]
        public long? relation;

    
        [JsonProperty("Value")]
        public long? value;

      
    }
}