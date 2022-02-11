using NUnit.Framework;
using ElasticsearchQuery;
using System.Linq.Expressions;
using System.Linq;
using System;
using System.Collections.Generic;
using Nest;
using ElasticsearchQueryLib.Tests;
using ElasticSearchQuery.Tests;

namespace ElasticsearchQueryLib.Tests
{
    class QueryTranslatorRangeQuriesTests
    {
        private QueryTranslator queryTranslator;
        private List<MockModel> model = new List<MockModel>();

        [SetUp]
        public void Setup()
        {
            queryTranslator = new QueryTranslator();
        }


        [Test]
        public void Translate_ExpressionWithWhereLessThenClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id < 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThan(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithWhereLessThenEqualToClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id <= 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThanOrEquals(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreaterThenClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id > 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThan(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id >= 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id >= 31 && x.Name == "test");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(31))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreTermsSame(expectedQuery, actualQuery));
        }

        // DateTime RangeQueries

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithWhereGreaterThenDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date > dateTime);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThan(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithWhereLessThenEqualDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date <= dateTime);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThanOrEquals(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }
        [Test]
        public void Translate_ExpressionWithWhereLessThenDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date < dateTime);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThan(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualDateTimeWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime && x.Name == "test");

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreTermsSame(expectedQuery, actualQuery));
        }
    }
}
