// -----------------------------------------------------------------------
// <copyright file="QueryTranslatorSortTests.cs" company="Enterprise Products Partners L.P. (Enterprise)">
// © Copyright 2012 - 2019, Enterprise Products Partners L.P. (Enterprise), All Rights Reserved.
// Permission to use, copy, modify, or distribute this software source code, binaries or
// related documentation, is strictly prohibited, without written consent from Enterprise.
// For inquiries about the software, contact Enterprise: Enterprise Products Company Law
// Department, 1100 Louisiana, 10th Floor, Houston, Texas 77002, phone 713-381-6500.
// </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using ElasticsearchQuery;
using ElasticsearchQuery.Tests.Models;
using Nest;
using NUnit.Framework;

namespace ElasticsearchQuery.Tests
{
    public class QueryTranslatorSortTests
    {
        private QueryTranslator _queryTranslator;
        private List<MockModel> _model = new List<MockModel>();

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithSingleSortAscClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.OrderBy(x => x.Id);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualReq = actual.SearchRequest;

            SearchRequest expectedReq = new SearchRequest();
            ISort sort = new FieldSort()
            {
                Field = "id",
                Order = 0
            };
            expectedReq.Sort = new List<ISort>
            {
                sort
            };

            Assert.IsTrue(QueryCompare.AreSortsSame(actualReq, expectedReq));
        }

        [Test]
        public void Translate_ExpressionWithSingleSortDescClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.OrderByDescending(x => x.Id);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualReq = actual.SearchRequest;

            SearchRequest expectedReq = new SearchRequest();
            ISort sort = new FieldSort()
            {
                Field = "id",
                Order = (SortOrder?)1
            };
            expectedReq.Sort = new List<ISort>
            {
                sort
            };

            Assert.IsTrue(QueryCompare.AreSortsSame(actualReq, expectedReq));
        }

        [Test]
        public void Translate_ExpressionWithMultipleSortClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.OrderByDescending(x => x.Id);
            query = query.OrderBy(x => x.Name);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualReq = actual.SearchRequest;

            SearchRequest expectedReq = new SearchRequest();
            ISort sort1 = new FieldSort()
            {
                Field = "id",
                Order = (SortOrder?)1
            };
            ISort sort2 = new FieldSort()
            {
                Field = "name",
                Order = (SortOrder?)0
            };
            expectedReq.Sort = new List<ISort>
            {
                sort1, sort2
            };

            Assert.IsTrue(QueryCompare.AreSortsSame(actualReq, expectedReq));
        }

        [Test]
        public void Translate_ExpressionWithMultipleSortAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            query = query.Where(x => (x.Id == 30 && x.Name == "test0") || (x.Id == 31 && x.Name == "test"));
            query = query.OrderByDescending(x => x.Id);
            query = query.OrderBy(x => x.Name);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualReq = actual.SearchRequest;

            var actualQuery = (IQueryContainer)actual.SearchRequest.Query;
            var expectedQuery = (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(30))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test0")))
                | (new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31))
                & new QueryContainerDescriptor<object>().Term(x => x.Field("name").Value("test")));

            SearchRequest expectedReq = new SearchRequest();
            ISort sort1 = new FieldSort()
            {
                Field = "id",
                Order = (SortOrder?)1
            };
            ISort sort2 = new FieldSort()
            {
                Field = "name",
                Order = (SortOrder?)0
            };
            expectedReq.Sort = new List<ISort>
            {
                sort1, sort2
            };

            Assert.IsTrue(QueryCompare.AreSortsSame(actualReq, expectedReq));
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(expectedQuery, actualQuery));
        }
    }
}
