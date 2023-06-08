using System.Collections.Generic;
using System.Linq;
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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");

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

            var actual = _queryTranslator.Translate(query.Expression, "mockmodel");
            var actualContainer = (IQueryContainer)actual.SearchRequest.Query;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));
        }
    }
}
