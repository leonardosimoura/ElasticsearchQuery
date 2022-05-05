// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Reflection;

namespace ElasticLinq.Request.Criteria
{
    /// <summary>
    /// Criteria that will match all documents.
    /// </summary>
    [DebuggerDisplay("collectioncontains")]
    class CollectionCompoundCriteria : CompoundCriteria, ICriteria
    {
        /// <summary>
        /// Get the single instance of the <see cref="MatchCriteria"/> class.
        /// </summary>

    
        
        public CollectionCompoundCriteria(string toInsert, IEnumerable<ICriteria> criteria, string pathName = null, bool isNested = false ) : base(criteria, pathName, isNested) {

            ToInsertField = toInsert;
        }

        /// <inheritdoc/>
        
        public string ToInsertField { get; set; }
        
        

        public override string Name => "collectioncompound";

        

        public override string ToString()
        {
            return $"{Name}";
        }


    }
}