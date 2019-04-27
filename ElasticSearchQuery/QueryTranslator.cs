using ElasticSearchQuery.Extensions;
using ElasticSearchQuery.Helpers;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ElasticSearchQuery
{
    internal class QueryTranslator : ExpressionVisitor
    {
        SearchRequest _searchRequest;
        QueryContainer queryContainer;
        string field = string.Empty;
        List<GroupBy> fieldsGroupBy = new List<GroupBy>();
        List<Aggregation> aggregations = new List<Aggregation>();
        string operacao = string.Empty;
        ExpressionType binaryExpType;
        object value = null;
        private bool AndCondition = true;
        Type elementType;


        private AggregationBase _aggregationBase;

        public AggregationBase AggregationBase
        {
            get { return _aggregationBase; }
            set {
                if (_aggregationBase == null)                
                    _aggregationBase = value;                
                else                
                    _aggregationBase = _aggregationBase && value;
            }
        }

        internal QueryTranslator()
        {

        }

        public AggregationDictionary ObterAgrupamentoNest(IEnumerable<GroupBy> agrupamentos, AggregationBase aggregations = null)
        {
            foreach (var aggr in agrupamentos)
            {

                //Tipos de agrupamento
                if (aggr.PropertyType == typeof(DateTime)
                    || aggr.PropertyType == typeof(DateTime?))
                {
                    var dateHistAgg = new DateHistogramAggregation(aggr.Field)
                    {
                        Missing = (DateTime?)null ,//(DateTime?)aggr.Missing,
                        Field = aggr.Field,
                        //Aggregations = ((aggregations != null) ? aggregations : null),
                        Interval = DateInterval.Day,
                        MinimumDocumentCount = 1,
                        //Script = (!string.IsNullOrWhiteSpace(aggr.Script)) ? new InlineScript(aggr.Script) : null
                    };

                    if (aggregations != null)
                        dateHistAgg.Aggregations = aggregations;

                    aggregations = dateHistAgg;
                }
                else
                {
                    var termsAgg = new TermsAggregation(aggr.Field)
                    {
                        Field = aggr.Field,
                       //Aggregations = ((aggregations != null) ? aggregations : null),
                        Size = int.MaxValue,
                        //Missing = null,//aggr.Missing,
                        MinimumDocumentCount = 1,
                        //Script = (!string.IsNullOrWhiteSpace(aggr.Script)) ? new InlineScript(aggr.Script) : null
                    };

                    if (aggregations != null)                    
                        termsAgg.Aggregations = aggregations;
                    

                    aggregations = termsAgg;
                }
            }
            return aggregations;
        }


        private void SetAggregation()
        {
            

            foreach (var item in aggregations)
            {
                switch (item.Method)
                {
                    case "Count":
                        AggregationBase = new ValueCountAggregation(item.AggName, item.Field);
                        break;
                    case "Min":
                        AggregationBase = new MinAggregation(item.AggName, item.Field);
                        break;
                    case "Max":
                        AggregationBase = new MaxAggregation(item.AggName, item.Field);
                        break;
                    case "Sum":
                        AggregationBase = new SumAggregation(item.AggName, item.Field);
                        break;
                    case "Average":
                        AggregationBase = new AverageAggregation(item.AggName, item.Field);
                        break;
                    default:
                        break;
                }
            }
            
            _searchRequest.Aggregations = ObterAgrupamentoNest(fieldsGroupBy, AggregationBase);
        }

        private void SetTextSearch()
        {
            //TODO Create a mapping so when is need a full text query we know
            switch (operacao)
            {
                case "Contains":
                    queryContainer = Query<object>.Wildcard(f => f.Value("*"+value.ToString()).Field(field));
                    break;
                case "StartsWith":
                    queryContainer = Query<object>.Prefix(f => f.Field(field).Value(value.ToString()));
                    break;
                case "EndsWith":
                    queryContainer = Query<object>.Regexp(f => f.Field(field).Value(".*"+value.ToString()));
                    break;
                default:
                    break;
            }            

            if (_searchRequest.Query == null)
            {
                _searchRequest.Query = queryContainer;
            }
            else
            {
                if (AndCondition)
                {
                    _searchRequest.Query = _searchRequest.Query && _searchRequest.Query;
                }
                else
                {
                    _searchRequest.Query = _searchRequest.Query || _searchRequest.Query;
                }
            }
        }

        private void SetQuery()
        {
            switch (binaryExpType)
            {
                case ExpressionType.Equal:
                    queryContainer = Query<object>.Term(t => t.Field(field).Value(value));
                    break;
                case ExpressionType.NotEqual:
                    queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Term(t => t.Field(field).Value(value))));
                    break;
                case ExpressionType.LessThan:
                    //TODO cast only when is necessary to double / decimal
                    queryContainer = Query<object>.Range(r => r.Field(field).LessThan((double?)((decimal?)value)));
                    break;
                case ExpressionType.LessThanOrEqual:
                    queryContainer = Query<object>.Range(r => r.Field(field).LessThanOrEquals((double?)((decimal?)value)));
                    break;
                case ExpressionType.GreaterThan:
                    queryContainer = Query<object>.Range(r => r.Field(field).GreaterThan((double?)((decimal?)value)));
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    queryContainer = Query<object>.Range(r => r.Field(field).GreaterThanOrEquals((double?)((decimal?)value)));
                    break;
                default:
                    break;
            }

            if (_searchRequest.Query == null)
            {
                _searchRequest.Query = queryContainer;
            }
            else
            {
                if (AndCondition)
                {
                    _searchRequest.Query = _searchRequest.Query && _searchRequest.Query;
                }
                else
                {
                    _searchRequest.Query = _searchRequest.Query || _searchRequest.Query;
                }
            }
        }

        internal QueryTranslateResult Translate(Expression expression, Type elementType)
        {
            this.elementType = elementType;            

            _searchRequest = new SearchRequest(elementType.Name.ToLower(), elementType.Name.ToLower());
            this.Visit(expression);
            _searchRequest.Size = 100;
            _searchRequest.From = 0;
            _searchRequest.Human = true;
            if (_searchRequest.Query == null)
                _searchRequest.Query = Query<object>.MatchAll();

            var result = new QueryTranslateResult(_searchRequest, fieldsGroupBy,aggregations);

            return result;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            switch (m.Method.Name)
            {
                case "Where":
                    LambdaExpression lambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);

                    this.Visit(lambda.Body);

                    return m;
                case "Count":
                case "Min":
                case "Max":
                case "Sum":
                case "Average":                    
                    var aggLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    if (aggLambda.Body is MemberExpression)
                    {
                        var memberExp = aggLambda.Body as MemberExpression;
                        aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                    }
                    SetAggregation();
                    return m;
                    break;
                case "Contains":
                    operacao = m.Method.Name;
                    field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                        value = (m.Arguments[0] as ConstantExpression).Value;

                    SetTextSearch();

                    return m;
                case "StartsWith":
                    operacao = m.Method.Name;
                    field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                        value = (m.Arguments[0] as ConstantExpression).Value;

                    SetTextSearch();

                    return m;
                    break;
                case "EndsWith":
                    operacao = m.Method.Name;
                    field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                        value = (m.Arguments[0] as ConstantExpression).Value;

                    SetTextSearch();

                    return m;
                    break;
                case "GroupBy":

                    LambdaExpression groupByLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    if (groupByLambda.Body is NewExpression)
                    {
                        var newExp = groupByLambda.Body as NewExpression;

                        foreach (var item in newExp.Members)
                        {
                            var pInfo = item as PropertyInfo;
                            fieldsGroupBy.Add(new GroupBy(item.Name.ToCamelCase(), pInfo.PropertyType));
                        }                      
                    }
                    else if (groupByLambda.Body is MemberExpression)
                    {
                        var memberExp = groupByLambda.Body as MemberExpression;
                        fieldsGroupBy.Add(new GroupBy(memberExp.Member.Name.ToCamelCase(), groupByLambda.Body.Type));
                    }

                    SetAggregation();
                  
                    return m;
                    break;
                case "Select":
                    LambdaExpression selectLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    if (m.Arguments[0] is MethodCallExpression)
                    {
                        var sMExp = m.Arguments[0] as MethodCallExpression;
                        if (sMExp.Method.Name == "GroupBy")
                        {
                            if (selectLambda.Body is NewExpression)
                            {
                                var sBodyNewExp = selectLambda.Body as NewExpression;

                                foreach (var argument in sBodyNewExp.Arguments)
                                {
                                    if (argument is MethodCallExpression)
                                    {
                                        var arMExp = argument as MethodCallExpression;
                                        var propArgLambda = (LambdaExpression)ExpressionHelper.StripQuotes(arMExp.Arguments[1]);
                                        var propArgMExp = propArgLambda.Body as MemberExpression;
                                        aggregations.Add(new Aggregation(propArgMExp.Member.Name.ToCamelCase(), arMExp.Method.Name));                                        
                                    }
                                }
                            }
                        }
                    }  

                    this.Visit(m.Arguments[0]);
                    return m;
                    break;
                default:
                    throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
                    break;
            }     
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:

                    this.Visit(u.Operand);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            this.Visit(b.Left);

            switch (b.NodeType)
            {
                case ExpressionType.And:
                    AndCondition = true;
                    break;
                case ExpressionType.Or:
                    AndCondition = false;
                    break;
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    binaryExpType = b.NodeType;
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            this.Visit(b.Right);

            SetQuery();

            return b;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Value == null)
            {
                queryContainer = Query<object>.Term(f => f.Field(field).Value(null));
            }
            else
            {
                switch (Type.GetTypeCode(c.Value.GetType()))
                {
                    case TypeCode.Object:
                        throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", c.Value));
                    default:
                        value = c.Value;
                        break;
                }
            }

            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
                field = m.Member.Name.ToCamelCase();
            }
            else if (m.Expression != null && m.Expression.NodeType == ExpressionType.Constant)
            {
                value = (m.Member as FieldInfo).GetValue(((m as MemberExpression).Expression as ConstantExpression).Value);
            }
            else if (m.Expression == null)
            {
                var field = m.Type.GetField(m.Member.Name);
                value = field.GetValue(null);
            }
            return m;            
        }
    }

    internal class QueryTranslateResult
    {
        public QueryTranslateResult(SearchRequest searchRequest, IEnumerable<GroupBy> groupBy, IEnumerable<Aggregation> aggregation)
        {
            SearchRequest = searchRequest;
            GroupBy = groupBy;
            Aggregation = aggregation;
        }

        public SearchRequest SearchRequest { get; private set; }

        public IEnumerable<GroupBy> GroupBy { get; private set; }

        public IEnumerable<Aggregation> Aggregation{ get; private set; }
    }

    internal class GroupBy
    {
        public GroupBy(string field, Type type)
        {
            Field = field;
            PropertyType = type;
        }

        public string Field { get; set; }
        public Type PropertyType { get; set; }
    }

    internal class Aggregation
    {
        public Aggregation(string field, string method)
        {
            Field = field;
            Method = method;
        }

        public string Field { get; set; }
        public string Method { get; set; }

        public string AggName => $"{Method}_{Field}";
    }
}
