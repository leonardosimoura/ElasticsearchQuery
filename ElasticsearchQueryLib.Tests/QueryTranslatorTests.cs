using NUnit.Framework;
using ElasticsearchQuery;
using System.Linq.Expressions;
using System.Linq;
using System;
using System.Collections.Generic;
using Nest;
using ElasticsearchQueryLib.Tests;

namespace ElasticSearchQuery.Tests
{
    public class QueryTranslatorTests
    {
        private QueryTranslator queryTranslator;
        private List<MockModel> model = new List<MockModel>();

        [SetUp]
        public void Setup()
        {
            queryTranslator = new QueryTranslator();
        }

        [Test]
        public void VisitConstant_ConstantExpressionGiven_ChangesQueryTranslatorObjectValuePropToConstant()
        {        
            object constant = 42.6;
            var constantExpression = Expression.Constant(constant);

            queryTranslator.Visit(constantExpression);

            Assert.IsTrue(queryTranslator.Value == constant);
        }

        [Test]
        public void Translate_ExpressionWithWhereEqualClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var query1 = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, ((IQueryContainer)query1)));
        }

        [Test]
        public void Translate_ExpressionWithWhereAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31 && x.Name == "test");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);
            var expectedQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31 || x.Name == "test");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);
            var expectedQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultipleAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => ((x.Id == 30 && x.Name == "test0") || (x.Id == 31 && x.Name == "test")));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);
            var expectedQuery = (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0")))
                | (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithMultipleWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 30);
            query = query.Where(x => x.Name == "test0");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);
            var expectedQuery = (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0")));
                
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }
    }
}