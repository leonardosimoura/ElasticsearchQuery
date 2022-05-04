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
    class CollectionContainsCriteria : CriteriaWrapper<CollectionContainsCriteria>, ICriteria
    {
        /// <summary>
        /// Get the single instance of the <see cref="MatchCriteria"/> class.
        /// </summary>

        public enum ComparisonType
        {
            Match = 0,
            Term = 1,
            LT = 2,
            LTE = 3,
            GT = 4,
            GTE = 5,
            NOT = 6

        }
        readonly ReadOnlyCollection<object> values;
        public CollectionContainsCriteria(string field, Type type, object value, ComparisonType comparisonType, string compV, string pathName = null, bool isNested = false ) : base(pathName, isNested) {
            
            values = new ReadOnlyCollection<object>(new[] { value });
            Field = field;
            Type = type;
            CompareValue = compV;
            ComparisonTypeValue = comparisonType;
        }

        /// <inheritdoc/>

        public object Value => values[0];
        public string CompareValue { get; set; }

        public ComparisonType ComparisonTypeValue { get; set; }
        public Type Type { get; set; }
        public string Name => "collectioncontains";
        public string Field { get; set; }

        public override string ToString()
        {
            return $"collectioncontains {Field}: {{query : {Value}}}";
        }


    }
}