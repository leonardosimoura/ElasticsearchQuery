// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Reflection;

namespace ElasticLinq.Request.Criteria
{
    /// <summary>
    /// Criteria that will match all documents.
    /// </summary>
    [DebuggerDisplay("match")]
    class MatchCriteria : CriteriaWrapper<MatchCriteria>, ICriteria
    {
        /// <summary>
        /// Get the single instance of the <see cref="MatchCriteria"/> class.
        /// </summary>

        readonly ReadOnlyCollection<object> values;
        public MatchCriteria(string field, object value, MemberInfo member, string pathName = null, bool isNested = false ) : base(pathName, isNested) {
            
            values = new ReadOnlyCollection<object>(new[] { value });
            Field = field;
            Member = member;
        }

        /// <inheritdoc/>

        public object Value => values[0];

        public MemberInfo Member { get; }
        public string Name => "match";
        public string Field { get; set; }

        public override string ToString()
        {
            return $"match {Field}: {{query : {Value}}}";
        }


    }
}