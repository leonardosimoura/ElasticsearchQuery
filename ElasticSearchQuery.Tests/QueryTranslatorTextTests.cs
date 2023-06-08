using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using ElasticsearchQuery.Tests.Models;
using Nest;
using NUnit.Framework;

namespace ElasticsearchQuery.Tests
{
    public class QueryTranslatorTextTests
    {
        private QueryTranslator _queryTranslator;
        private List<MockModel> _model = new List<MockModel>();

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithContainsClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.Contains("jg"));
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query).Match;

            var expectedQuery = new QueryContainerDescriptor<object>().Match(x => x.Field("name").Query("jg"));

            Assert.IsTrue(actualQuery.Field == "name");
            Assert.IsTrue(actualQuery.Query == "jg");
        }

        [Test]
        public void Translate_ExpressionWithStartsWithClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.StartsWith("jg"));
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Prefix(x => x.Field("name").Value("jg"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithContainsAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.Contains("jg") && x.Id == 31);
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Match(x => x.Field("name").Query("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithContainsAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.Contains("jg") || x.Id == 31);
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Match(x => x.Field("name").Query("jg"))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithStartWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.StartsWith("jg") && x.Id == 31);
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Prefix(x => x.Field("name").Value("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithStartWithsAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Name.StartsWith("jg") || x.Id == 31);
            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Prefix(x => x.Field("name").Value("jg"))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithStartsWithWithNestedAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.Name.StartsWith("jg") && x.Id == 30) || (x.Name == "test0" && (x.Id == 31 || x.Name == "test")));

            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = (new QueryContainerDescriptor<object>().Prefix(x => x.Field("name").Value("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithContainsWithNestedAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.Name.Contains("jg") && x.Id == 30) || (x.Name == "test0" && (x.Id == 31 || x.Name == "test")));

            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = (new QueryContainerDescriptor<object>().Match(x => x.Field("name").Query("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithStartsWithWithNestedAndOrRangeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            var dateTime = DateTime.Now;
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.Name.StartsWith("jg") && x.Id == 30) || (x.Name == "test0" && ((x.Id <= 31) || x.Name == "test")));

            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = (new QueryContainerDescriptor<object>().Prefix(x => x.Field("name").Value("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThanOrEquals(31))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }

        [Test]
        public void Translate_ExpressionWithContainsWithNestedAndOrRangeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            var dateTime = DateTime.Now;
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.Name.Contains("jg") && x.Id == 30) || (x.Name == "test0" && ((x.Id <= 31) || x.Name == "test")));

            Expression exp = query.Expression;
            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = (new QueryContainerDescriptor<object>().Match(x => x.Field("name").Query("jg"))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThanOrEquals(31))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualQuery, (IQueryContainer)expectedQuery));
        }
    }
}
