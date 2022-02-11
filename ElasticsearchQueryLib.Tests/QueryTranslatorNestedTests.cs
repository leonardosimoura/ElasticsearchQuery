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
    public class QueryTranslatorNestedTests
    {
        private QueryTranslator queryTranslator;
        private List<NestedMockModel> model = new List<NestedMockModel>();

        [SetUp]
        public void Setup()
        {
            queryTranslator = new QueryTranslator();
        }


        [Test]
        public void Translate_ExpressionWithNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndFlatWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31) && x.ProductName == "product");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("productName").Value("product")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndFlatWithOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31) || x.Id == 32);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(32)));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndtWithAndClauseInNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31 && x.Name == "test"));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1 & intermedidateQuery2)));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndtWithOrClauseInNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31 || x.Name == "test"));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1 | intermedidateQuery2)));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultipleNestedAndGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31) && x.MockModels.Any(x => x.Name == "test"));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1)))
                & (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2)));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithMultipleNestedOrGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(x => x.Id == 31) || x.MockModels.Any(x => x.Name == "test"));

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1)))
                | (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2)));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

    }
}