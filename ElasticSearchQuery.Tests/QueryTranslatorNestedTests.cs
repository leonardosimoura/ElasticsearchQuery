// -----------------------------------------------------------------------
// <copyright file="QueryTranslatorNestedTests.cs" company="Enterprise Products Partners L.P. (Enterprise)">
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
using ElasticsearchQuery;
using ElasticsearchQuery.Tests.Models;
using Nest;
using NUnit.Framework;

namespace ElasticsearchQuery.Tests
{
    public class QueryTranslatorNestedTests
    {
        private QueryTranslator _queryTranslator;
        private List<NestedMockModel> _model = new List<NestedMockModel>();

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndFlatWithAndClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31) && x.Name == "product");

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("product"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndFlatWithOrClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31) || x.Id == 32);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels").Query(y => intermedidateQuery))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(32));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndtWithAndClauseInNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31 && y.Name == "test"));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1 & intermedidateQuery2));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndtWithOrClauseInNestedGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31 || y.Name == "test"));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1 | intermedidateQuery2));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultipleNestedAndGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31) && x.MockModels.Any(y => y.Name == "test"));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                & new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultipleNestedOrGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31) || x.MockModels.Any(y => y.Name == "test"));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                | new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithNestedAndMultiplrAndOrGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.MockModels.Any(y => y.Name == "test") && x.Id == 3543) || x.Name == "test0");

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));

            var expectedQuery = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(3543)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultileWhereNestedAndMultiplrAndOrGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            query = query.Where(x => x.MockModels.Any(y => y.Id == 31) || x.MockModels.Any(y => y.Name == "test"));
            query = query.Where(x => (x.MockModels.Any(y => y.Name == "test1") && x.Id == 3543) || x.Name == "test0");

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));
            var intermedidateQuery3 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test1"));

            var intermedidateQuery4 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                | new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));
            var intermedidateQuery5 = (new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery3))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(3543)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"));

            Assert.IsTrue(QueryCompare.AreQueryContainersSame(intermedidateQuery4 & intermedidateQuery5, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultileWhereNestedAndMultiplrAndOrRangeGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.MockModels.Any(y => y.Id == 32) || x.MockModels.Any(y => y.Name == "test"));
            query = query.Where(x => (x.MockModels.Any(y => y.Id == 31 || y.Name == "test1") && x.Id >= 3543) || (x.Name == "test0"
                && (x.Date < dateTime || x.Id == 30)));

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(32));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));
            var intermedidateQuery3 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery4 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test1"));

            var intermedidateQuery5 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                | new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));
            var intermedidateQuery6 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery3 | intermedidateQuery4));
            var intermedidateQuery7 = (intermedidateQuery6
                & new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(3543)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThan(dateTime))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)));
            var expectedQuery = intermedidateQuery5 & intermedidateQuery7;
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }

        [Test]
        public void Translate_ExpressionWithMultileWhereNestedAndMultiplrAndOrRangeMultipleSortGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.MockModels.Any(y => y.Id == 32) || x.MockModels.Any(y => y.Name == "test"));
            query = query.Where(x => (x.MockModels.Any(y => y.Id == 31 || y.Name == "test1") && x.Id >= 3543) || (x.Name == "test0"
                && (x.Date < dateTime || x.Id == 30)));
            query = query.OrderBy(x => x.Id).ThenByDescending(x => x.Date);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(32));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));
            var intermedidateQuery3 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery4 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test1"));

            var intermedidateQuery5 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                | new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));
            var intermedidateQuery6 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery3 | intermedidateQuery4));
            var intermedidateQuery7 = (intermedidateQuery6
                & new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(3543)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThan(dateTime))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)));

            SearchRequest expectedReq = new SearchRequest();
            ISort sort1 = new FieldSort()
            {
                Field = "id",
                Order = (SortOrder?)0
            };
            ISort sort2 = new FieldSort()
            {
                Field = "date",
                Order = (SortOrder?)1
            };
            expectedReq.Sort = new List<ISort>
            {
                sort1, sort2
            };

            var expectedQuery = intermedidateQuery5 & intermedidateQuery7;
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreSortsSame(actual.SearchRequest, expectedReq));
        }

        [Test]
        public void Translate_ExpressionWithMultileWhereNestedAndMultiplrAndOrRangeMultipleSortPaginationGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new NestedMockModel();
            IQueryable<NestedMockModel> query = _model.AsQueryable();
            var dateTime = DateTime.Now;
            query = query.Where(x => x.MockModels.Any(y => y.Id == 32) || x.MockModels.Any(y => y.Name == "test"));
            query = query.Where(x => (x.MockModels.Any(y => y.Id == 31 || y.Name == "test1") && x.Id >= 3543) || (x.Name == "test0"
                && (x.Date < dateTime || x.Id == 30)));
            query = query.OrderBy(x => x.Id).ThenByDescending(x => x.Date);
            var top = 100;
            var from = 150;
            query = query.Skip(from);
            query = query.Take(top);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;

            var intermedidateQuery1 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(32));
            var intermedidateQuery2 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test"));
            var intermedidateQuery3 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.id").Value(31));
            var intermedidateQuery4 = new QueryContainerDescriptor<object>().Term(x => x.Field("mockModels.name").Value("test1"));

            var intermedidateQuery5 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery1))
                | new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery2));
            var intermedidateQuery6 = new QueryContainerDescriptor<object>().Nested(x => x.Path("mockModels")
                .Query(y => intermedidateQuery3 | intermedidateQuery4));
            var intermedidateQuery7 = (intermedidateQuery6
                & new QueryContainerDescriptor<object>().Range(x => x.Field("id").GreaterThanOrEquals(3543)))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0"))
                & (new QueryContainerDescriptor<object>().DateRange(x => x.Field("date").LessThan(dateTime))
                | new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30)));

            SearchRequest expectedReq = new SearchRequest();
            ISort sort1 = new FieldSort()
            {
                Field = "id",
                Order = (SortOrder?)0
            };
            ISort sort2 = new FieldSort()
            {
                Field = "date",
                Order = (SortOrder?)1
            };
            expectedReq.Sort = new List<ISort>
            {
                sort1, sort2
            };

            var expectedQuery = intermedidateQuery5 & intermedidateQuery7;
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
            Assert.IsTrue(QueryCompare.AreSortsSame(actual.SearchRequest, expectedReq));
            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
        }
    }
}
