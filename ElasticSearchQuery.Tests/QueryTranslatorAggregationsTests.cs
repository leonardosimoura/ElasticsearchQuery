using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using ElasticsearchQuery.Tests.Models;
using Nest;
using NUnit.Framework;

namespace ElasticsearchQuery.Tests
{
    public class QueryTranslatorAggregationsTests
    {
        private QueryTranslator _queryTranslator;
        private List<NestedMockModel> _model = new List<NestedMockModel>();

        public Expression ReturnAggregateExpression(IQueryable source, string member, string aggFunc)
        {
            PropertyInfo property = source.ElementType.GetProperty(member);
            FieldInfo field = source.ElementType.GetField(member);
            ParameterExpression parameter = Expression.Parameter(source.ElementType, "f");
            Expression selector = Expression.Lambda(Expression.MakeMemberAccess(parameter, (MemberInfo)property ?? field), parameter);
            List<MethodInfo> methods = typeof(Queryable).GetMethods().Where(
                m => m.Name == aggFunc
                     && m.IsGenericMethod).ToList();

            var callExpression = source.Expression;
            foreach (var method in methods)
            {
                try
                {
                    var genericMethod = aggFunc == "Sum" || aggFunc == "Average" ? method.MakeGenericMethod(new[] { source.ElementType }) :
                        method.MakeGenericMethod(new[] { source.ElementType, property.PropertyType });

                    callExpression = Expression.Call(
                        null,
                        genericMethod,
                        new[] { source.Expression, Expression.Quote(selector) });
                    return callExpression;
                }
                catch (ArgumentException e)
                {
                    continue;
                }
            }
            throw new ArgumentException("Not a valid aggregate");
        }

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_SumAggregation()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var queryExpression = ReturnAggregateExpression(query, "Id", "Sum");

            var actual = _queryTranslator.Translate(queryExpression, "nestedmockmodel");
            var actualQuery = actual.SearchRequest.Aggregations;

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedDictionary = new AggregationDictionary();
            expectedDictionary.Add("Sum_id", new AggregationContainer
            {
                Sum = new SumAggregation("Sum_id", "id")
            });

            Assert.IsTrue(QueryCompare.AreAggregationDictionarySame(expectedDictionary, actualQuery));
        }

        [Test]
        public void Translate_MinAggregation()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var queryExpression = ReturnAggregateExpression(query, "Id", "Min");

            var actual = _queryTranslator.Translate(queryExpression, "nestedmockmodel");
            var actualQuery = actual.SearchRequest.Aggregations;

            var expectedDictionary = new AggregationDictionary();
            expectedDictionary.Add("Min_id", new AggregationContainer
            {
                Min = new MinAggregation("Min_id", "id")
            });

            Assert.IsTrue(QueryCompare.AreAggregationDictionarySame(expectedDictionary, actualQuery));
        }

        [Test]
        public void Translate_MaxAggregation()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var queryExpression = ReturnAggregateExpression(query, "Id", "Max");

            var actual = _queryTranslator.Translate(queryExpression, "nestedmockmodel");
            var actualQuery = actual.SearchRequest.Aggregations;

            var expectedDictionary = new AggregationDictionary();
            expectedDictionary.Add("Max_id", new AggregationContainer
            {
                Max = new MaxAggregation("Max_id", "id")
            });

            Assert.IsTrue(QueryCompare.AreAggregationDictionarySame(expectedDictionary, actualQuery));
        }

        [Test]
        public void Translate_AverageAggregation()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var queryExpression = ReturnAggregateExpression(query, "Id", "Average");

            var actual = _queryTranslator.Translate(queryExpression, "nestedmockmodel");
            var actualQuery = actual.SearchRequest.Aggregations;

            var expectedDictionary = new AggregationDictionary();
            expectedDictionary.Add("Average_id", new AggregationContainer
            {
                Average = new AverageAggregation("Average_id", "id")
            });

            Assert.IsTrue(QueryCompare.AreAggregationDictionarySame(expectedDictionary, actualQuery));
        }
    }
}
