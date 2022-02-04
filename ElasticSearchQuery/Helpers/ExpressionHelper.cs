using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ElasticsearchQuery.Helpers
{
    internal static class ExpressionHelper
    {
        internal static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {

                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }
    }
}
