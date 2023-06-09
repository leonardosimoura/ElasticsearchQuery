﻿using ElasticsearchQuery.Helpers;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ElasticsearchQuery
{
    public abstract class QueryProvider : IQueryProvider
    {
        protected QueryProvider()
        {

        }

        IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
        {
            return new ElasticQuery<S>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);

            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(ElasticQuery<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        S IQueryProvider.Execute<S>(Expression expression)
        {
            return (S)this.Execute(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return this.Execute(expression);
        }

        public abstract object Execute(Expression expression);
    }
}
