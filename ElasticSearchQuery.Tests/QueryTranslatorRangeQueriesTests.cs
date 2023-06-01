// -----------------------------------------------------------------------
// <copyright file="QueryTranslatorRangeQuriesTests.cs" company="Enterprise Products Partners L.P. (Enterprise)">
// © Copyright 2012 - 2019, Enterprise Products Partners L.P. (Enterprise), All Rights Reserved.
// Permission to use, copy, modify, or distribute this software source code, binaries or
// related documentation, is strictly prohibited, without written consent from Enterprise.
// For inquiries about the software, contact Enterprise: Enterprise Products Company Law
// Department, 1100 Louisiana, 10th Floor, Houston, Texas 77002, phone 713-381-6500.
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using ElasticsearchQuery.Tests.Models;
using Nest;
using NUnit.Framework;

namespace ElasticsearchQuery.Tests
{
    public class QueryTranslatorRangeQueriesTests
    {
        private QueryTranslator _queryTranslator;
        private List<MockModel> _model = new List<MockModel>();
        private List<NestedMockModel> _nestedModel = new List<NestedMockModel>();

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithWhereLessThenClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Id < 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThan(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereLessThenEqualToClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Id <= 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThanOrEquals(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreaterThenClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Id > 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThan(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Id >= 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(31));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => x.Id >= 31 && x.Name == "test");

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(31))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreRangeQueriesSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreTermsSame(expectedQuery, actualQuery));
        }

        // DateTime RangeQueries
        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreaterThenDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date > dateTime);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThan(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereLessThenEqualDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date <= dateTime);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThanOrEquals(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereLessThenDateTimeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date < dateTime);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThan(dateTime));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithWhereGreateThenEqualDateTimeWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime && x.Name == "test");

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"));

            Assert.IsTrue(QueryCompare.AreDateRangeQueriesSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreTermsSame(expectedQuery, actualQuery));
        }

        // Combination of two multiple range clauses
        [Test]
        public void Translate_ExpressionWithMultipleRangeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime && x.Id < 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime))
                & new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThan(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultipleRangeAndOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _nestedModel.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.Date >= dateTime && x.Name == "test" && x.Id < 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var expectedQuery = new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").GreaterThanOrEquals(dateTime))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test"))
                & new QueryContainerDescriptor<object>().Range(x => x.Field("id").LessThan(31));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }
    }
}
