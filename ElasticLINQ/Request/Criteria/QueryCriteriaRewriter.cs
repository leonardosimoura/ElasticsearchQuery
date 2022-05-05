// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using System;
using System.Linq;

namespace ElasticLinq.Request.Criteria
{
    /// <summary>
    /// Query DSL is slightly different from Filter DSL. In order to keep
    /// code paths simple we always build as if doing a filter. Here we
    /// rewrite the <see cref="ICriteria" /> designed for a filter into a query.
    /// </summary>
    static class QueryCriteriaRewriter
    {
        /// <summary>
        /// Take an <see cref="ICriteria" /> for filtering and return an <see cref="ICriteria" />
        /// with any necessary compensations for querying.
        /// </summary>
        /// <param name="criteria"><see cref="ICriteria" /> built for filtering.</param>
        /// <returns><see cref="ICriteria" /> built for querying.</returns>
        public static ICriteria Compensate(ICriteria criteria)
        {
            if (criteria is OrCriteria)
                return Rewrite((OrCriteria)criteria);

            if (criteria is AndCriteria)
                return Rewrite((AndCriteria)criteria);

            if (criteria is NotCriteria)
                return Rewrite((NotCriteria)criteria);

            if (criteria is ConstantCriteria)
                return Rewrite((ConstantCriteria)criteria);
            if (criteria is CollectionCompoundCriteria)
                return Rewrite((CollectionCompoundCriteria)criteria);

            return criteria;
        }

        /// <summary>
        /// Rewrite a <see cref="NotCriteria" /> as a <see cref="BoolCriteria" />.
        /// </summary>
        /// <param name="not">NotCriteria to rewrite.</param>
        /// <returns><see cref="BoolCriteria" /> with the criteria from Not mapped into MustNot.</returns>
        static BoolCriteria Rewrite(NotCriteria not)
        {
            var mustNotCriteria = not.Criteria is OrCriteria
                ? ((OrCriteria) not.Criteria).Criteria
                : Enumerable.Repeat(not.Criteria, 1);
            return new BoolCriteria(null, null, mustNotCriteria.Select(Compensate), not.Path, not.Nested);
        }

        /// <summary>
        /// Rewrite an <see cref="OrCriteria" /> as a <see cref="BoolCriteria" />.
        /// </summary>
        /// <param name="or"><see cref="OrCriteria" /> to rewrite.</param>
        /// <returns><see cref="BoolCriteria" /> with the criteria from the Or mapped into Should.</returns>
        static BoolCriteria Rewrite(OrCriteria or)
        {
            return new BoolCriteria(null, or.Criteria.Select(Compensate), null, or.Path, or.Nested);
        }

        /// <summary>
        /// Rewrite an <see cref="AndCriteria" /> as a <see cref="BoolCriteria" />.
        /// </summary>
        /// <param name="and"><see cref="AndCriteria" /> to rewrite.</param>
        /// <returns><see cref="BoolCriteria" /> with the criteria from the And mapped into Must.</returns>
        static BoolCriteria Rewrite(AndCriteria and)
        {
            var mustNot = and.Criteria.OfType<NotCriteria>().ToList();

            var orCriteria = and.Criteria.OfType<OrCriteria>().ToArray();
            var canFlattenOrCriteria = orCriteria.Length == 1;

            var shouldCriteria = (canFlattenOrCriteria ? orCriteria.SelectMany(o => o.Criteria) : Enumerable.Empty<OrCriteria>()).ToList();
            var must = and.Criteria.Except(mustNot).Except(shouldCriteria);

            return new BoolCriteria(must.Select(Compensate),
                            shouldCriteria,
                            mustNot.Select(c => c.Criteria).Select(Compensate), and.Path, and.Nested);
        }

        /// <summary>
        /// Rewrite a <see cref="ConstantCriteria" /> as a <see cref="MatchAllCriteria" /> that might be 
        /// wrapped in a <see cref="NotCriteria" /> depending on whether it is true or false respectively.
        /// </summary>
        /// <param name="constant"><see cref="ConstantCriteria" /> to rewrite.</param>
        /// <returns>
        /// <see cref="MatchAllCriteria" /> if true; otherwise a <see cref="MatchAllCriteria" /> 
        /// wrapped in a <see cref="NotCriteria" /> if false.
        /// </returns>
        static ICriteria Rewrite(ConstantCriteria constant)
        {
            return constant == ConstantCriteria.True ? MatchAllCriteria.Instance : NotCriteria.Create(MatchAllCriteria.Instance);
        }

        static void UpdateCriteria(ICriteria criteria, string toInsertNest = null)
        {
            if (criteria is TermCriteria)
            {
                var tc = (TermCriteria)criteria;
                if (!string.IsNullOrEmpty(toInsertNest))
                    tc.Field = $"{toInsertNest}.{tc.Field}";

            }
            if (criteria is MatchCriteria)
            {
                var tc = (MatchCriteria)criteria;
                if (!string.IsNullOrEmpty(toInsertNest))
                    tc.Field = $"{toInsertNest}.{tc.Field}";
            }
            if (criteria is TermsCriteria)
            {
                var tc = (TermsCriteria)criteria;
                if (!string.IsNullOrEmpty(toInsertNest))
                    tc.Field = $"{toInsertNest}.{tc.Field}";
            }
            if (criteria is OrCriteria)
            {
                var tc = (OrCriteria)criteria;
                foreach (var innerCriteria in tc.Criteria)
                {
                    UpdateCriteria(innerCriteria, toInsertNest);
                }
            }
            if (criteria is AndCriteria)
            {
                var tc = (AndCriteria)criteria;
                foreach (var innerCriteria in tc.Criteria)
                {
                    UpdateCriteria(innerCriteria, toInsertNest);
                }
            }
           

        }

        static ICriteria Rewrite(CollectionCompoundCriteria and)
        {
            if (and.Criteria.Count > 1)
            {
                throw new Exception("CollectionCompoundCriteria contains more than 1 criteria");
            }
            if (and.Criteria.Count == 0)
            {
                throw new Exception("CollectionCompoundCriteria does not contain any criteria");
                
            }
            var cm = and.Criteria.First();


            if (cm is OrCriteria)
            {
                var tc = (OrCriteria)cm;
                tc.Path = and.Path;
                tc.Nested = and.Nested;

                foreach (var innerCriteria in tc.Criteria)
                {
                    UpdateCriteria(innerCriteria, and.ToInsertField);
                }
                return Rewrite(tc);
            }

            if (cm is AndCriteria)
            {
                var tc = (AndCriteria)cm;
                tc.Path = and.Path;
                tc.Nested = and.Nested;

                foreach (var innerCriteria in tc.Criteria)
                {
                    UpdateCriteria(innerCriteria, and.ToInsertField);
                }
                return Rewrite(tc);
            }
            if (cm is NotCriteria)
            {
                var tc = (NotCriteria)cm;
                tc.Path = and.Path;
                tc.Nested = and.Nested;
                UpdateCriteria(tc, and.ToInsertField);


                return Rewrite(tc);
            }
            else
            {
                UpdateCriteria(cm, and.ToInsertField);
                if (cm is TermCriteria)
                {
                    var tc = (TermCriteria)cm;
                    tc.Path = and.Path;
                    tc.Nested = and.Nested;
                    return tc;
                }
                if (cm is TermsCriteria)
                {
                    var tc = (TermsCriteria)cm;
                    tc.Path = and.Path;
                    tc.Nested = and.Nested;
                    return tc;
                }
                if (cm is MatchCriteria)
                {
                    var tc = (MatchCriteria)cm;
                    tc.Path = and.Path;
                    tc.Nested = and.Nested;
                    return tc;
                }

            }

            return cm;

        }
    }
}