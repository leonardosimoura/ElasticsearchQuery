// -----------------------------------------------------------------------
// <copyright file="QueryTranslatorPaginationTests.cs" company="Enterprise Products Partners L.P. (Enterprise)">
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
    public class QueryTranslatorPaginationTests
    {
        private QueryTranslator _queryTranslator;
        private List<MockModel> _model = new List<MockModel>();

        [SetUp]
        public void Setup()
        {
            _queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithTakeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var size = 100;
            query = query.Take(size);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualSize = actual.SearchRequest.Size;

            Assert.IsTrue(actualSize == size);
        }

        [Test]
        public void Translate_ExpressionWithTakeAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var size = 100;
            query = query.Take(size);
            query = query.Where(x => x.Id == 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualContainer = (IQueryContainer)actual.SearchRequest.Query;
            var actualSize = actual.SearchRequest.Size;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actualSize == size);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));
        }

        [Test]
        public void Translate_ExpressionWithSkipClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var size = 100;
            query = query.Skip(size);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualSize = actual.SearchRequest.From;

            Assert.IsTrue(actualSize == size);
        }

        [Test]
        public void Translate_ExpressionWithSkipAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var size = 100;
            query = query.Skip(size);
            query = query.Where(x => x.Id == 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualContainer = (IQueryContainer)actual.SearchRequest.Query;
            var actualSize = actual.SearchRequest.From;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actualSize == size);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));
        }

        [Test]
        public void Translate_ExpressionWithSkipAndFromClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var top = 100;
            var from = 150;
            query = query.Skip(from);
            query = query.Take(top);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());

            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
        }

        [Test]
        public void Translate_ExpressionWithSkipFromAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = _model.AsQueryable();
            var top = 100;
            var from = 150;
            query = query.Skip(from);
            query = query.Take(top);
            query = query.Where(x => x.Id == 31);

            var actual = _queryTranslator.Translate(query.Expression, obj.GetType().ToString());
            var actualContainer = (IQueryContainer)actual.SearchRequest.Query;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));
        }
    }
}
