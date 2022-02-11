using ElasticsearchQuery;
using ElasticSearchQuery.Tests;
using Nest;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace ElasticsearchQueryLib.Tests
{
    public class QueryTranslatorPaginationTests
    {
        private QueryTranslator queryTranslator;
        private List<MockModel> model = new List<MockModel>();
        [SetUp]
        public void Setup()
        {
            queryTranslator = new QueryTranslator();
        }

        [Test]
        public void Translate_ExpressionWithTakeClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var size = 100;
            query = query.Take(size);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualSize = actual.SearchRequest.Size;

            Assert.IsTrue(actualSize == size);
        }

        [Test]
        public void Translate_ExpressionWithTakeAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var size = 100;
            query = query.Take(size);
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualContainer = ((IQueryContainer)actual.SearchRequest.Query);
            var actualSize = actual.SearchRequest.Size;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actualSize == size);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));

        }

        [Test]
        public void Translate_ExpressionWithSkipClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var size = 100;
            query = query.Skip(size);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualSize = actual.SearchRequest.From;

            Assert.IsTrue(actualSize == size);
        }

        [Test]
        public void Translate_ExpressionWithSkipAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var size = 100;
            query = query.Skip(size);
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualContainer = ((IQueryContainer)actual.SearchRequest.Query);
            var actualSize = actual.SearchRequest.From;

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actualSize == size);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));

        }

        [Test]
        public void Translate_ExpressionWithSkipAndFromClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var top = 100;
            var from = 150;
            query = query.Skip(from);
            query = query.Take(top);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
        }

        [Test]
        public void Translate_ExpressionWithSkipFromAndWhereClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            var top = 100;
            var from = 150;
            query = query.Skip(from);
            query = query.Take(top);
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            var actualContainer = ((IQueryContainer)actual.SearchRequest.Query);

            var expectedContainer = new QueryContainerDescriptor<object>().Term(x => x.Field("id").Value(31));

            Assert.IsTrue(actual.SearchRequest.Size == top);
            Assert.IsTrue(actual.SearchRequest.From == from);
            Assert.IsTrue(QueryCompare.AreQueryContainersSame(actualContainer, expectedContainer));
        }
    }
}