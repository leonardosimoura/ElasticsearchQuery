using NUnit.Framework;
using ElasticsearchQuery;
using System.Linq.Expressions;
using System.Linq;
using System;
using System.Collections.Generic;
using Nest;

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
            //Arrange            
            object constant = 42.6;
            var constantExpression = Expression.Constant(constant);

            //Act
            queryTranslator.Visit(constantExpression);

            //Assert
            Assert.IsTrue(queryTranslator.Value == constant);
        }

        [Test]
        public void Translate_ExpressionWithWhereEqualClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());
            
            // Would be better if we create the query using nest and then compare them
            var termQuery = ((IQueryContainer)actual.SearchRequest.Query).Term;
            //var termQuery1 = ((IQueryContainer)((IQueryContainer)actual.SearchRequest.Query).Bool.Must.First()).Term;

            Assert.AreEqual(termQuery.Field.ToString(), "id");
            Assert.AreEqual(termQuery.Value, 31);


        }

        /*[Test]
        public void Translate_ExpressionWithWhereLessThenClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var termQuery = ((IQueryContainer)actual.SearchRequest.Query).Term;
            Assert.AreEqual(termQuery.Field.ToString(), "id");
            Assert.AreEqual(termQuery.Value, 31);

        }
        [Test]
        public void Translate_MethodCallExpressionWithWhereEqualClauseGiven_ReturnsObjectHavingRespectiveNestQuery()
        {
            var obj = new MockModel();
            IQueryable<MockModel> query = model.AsQueryable();
            query = query.Where(x => x.Id == 31);

            var actual = queryTranslator.Translate(query.Expression, obj.GetType());

            var termQuery = ((IQueryContainer)actual.SearchRequest.Query).Term;
            Assert.AreEqual(termQuery.Field.ToString(), "id");
            Assert.AreEqual(termQuery.Value, 31);

        }*/

        /*[Test]
        public void VisitMember_MemberExpressionGiven_ChangesQueryTranslatorObjectFieldPropToMember()
        {
            //Arrange            
            object member = "hkjhk";
            MemberExpression memberExpression = "x";
            //Act
            queryTranslator.Visit(constantExpression);
            //Assert
            Assert.IsTrue(queryTranslator.value == constant);
        }
        [Test]
        public void SetQuery__ReturnsNestQueryContainerWithRespectiveTermQuery()
        {
            queryTranslator.Visit(Expression.con)
            queryTranslator.binaryExpType = ExpressionType.Equal;
            var actual = queryTranslator.SetQuery();
            actual
        }*/

        [Test]
        public void Test()
        {
            // Arrange

            // Act

            // Arrange
        }
    }
}