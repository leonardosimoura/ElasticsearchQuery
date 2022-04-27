// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

namespace ElasticLinq.Request.Criteria
{
    /// <summary>
    /// Criteria that selects documents if they have any value
    /// in the specified field.
    /// </summary>
    public class ExistsCriteria : SingleFieldCriteria, INegatableCriteria
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ExistsCriteria"/> class.
        /// </summary>
        /// <param name="field">Field that must exist for this criteria to be satisfied.</param>
        /// <param name="pathName"></param>
        /// <param name="isNested"></param>
        public ExistsCriteria(string field, string pathName = null, bool isNested =false )
            : base(field, pathName, isNested)
        {
        }

        /// <inheritdoc/>
        public override string Name => "exists";

        /// <summary>
        /// Negate this Exists criteria by turning it into a Missing criteria.
        /// </summary>
        /// <returns>Missing criteria for this field.</returns>
        public ICriteria Negate()
        {
            return new MissingCriteria(Field);
        }
    }
}