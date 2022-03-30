﻿using ElasticsearchQuery.Extensions;
using ElasticsearchQuery.Helpers;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ElasticsearchQuery
{
    internal class QueryTranslator : ExpressionVisitor
    {
        SearchRequest _searchRequest;
        QueryContainer queryContainer;
        string field = string.Empty;
        List<GroupBy> fieldsGroupBy = new List<GroupBy>();
        List<Aggregation> aggregations = new List<Aggregation>();
        List<OrderBy> fieldsOrderBy = new List<OrderBy>();
        private bool returnNumberOfRows = false;
        string operacao = string.Empty;
        ExpressionType binaryExpType;
        object value = null;
        private bool AndCondition = true;
        Type elementType;
        bool denyCondition = false;


        private AggregationBase _aggregationBase;

        public AggregationBase AggregationBase
        {
            get { return _aggregationBase; }
            set
            {
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
                        Missing = (DateTime?)null,//(DateTime?)aggr.Missing,
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
            switch (operacao)
            {
                case "Contains":

                    Func<WildcardQueryDescriptor<object>, IWildcardQuery> _containsSelector = f => f.Value("*" + value.ToString()).Field(field);

                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Wildcard(_containsSelector)));
                    else
                        queryContainer = Query<object>.Wildcard(_containsSelector);
                    break;
                case "StartsWith":

                    Func<PrefixQueryDescriptor<object>, IPrefixQuery> _startsWithSelector = f => f.Field(field).Value(value.ToString());

                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Prefix(_startsWithSelector)));
                    else
                        queryContainer = Query<object>.Prefix(_startsWithSelector);
                    break;
                case "EndsWith":

                    Func<RegexpQueryDescriptor<object>, IRegexpQuery> _endsWithSelector = f => f.Field(field).Value(".*" + value.ToString());

                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Regexp(_endsWithSelector)));
                    else
                        queryContainer = Query<object>.Regexp(_endsWithSelector);
                    break;
                case "MatchPhrase":

                    Func<MatchPhraseQueryDescriptor<object>, IMatchPhraseQuery> _matchPhraseSelector = f => f.Field(field).Query(value.ToString());

                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.MatchPhrase(_matchPhraseSelector)));
                    else
                        queryContainer = Query<object>.MatchPhrase(_matchPhraseSelector);
                    break;
                case "MultiMatch":
                    var _fields = field.Split(';');

                    Func<MultiMatchQueryDescriptor<object>, IMultiMatchQuery> _multiMachSelector = f => f.Fields(_fields).Query(value.ToString());

                    if (denyCondition)                    
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.MultiMatch(_multiMachSelector)));                    
                    else                    
                        queryContainer = Query<object>.MultiMatch(_multiMachSelector);                    

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
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(value.GetType()))
                    {
                        var _tempCollection = value as System.Collections.IEnumerable;

                        Func<TermsQueryDescriptor<object>, ITermsQuery> _termsSelector = t => t.Field(field).Terms(_tempCollection);

                        if (denyCondition)
                            queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Terms(_termsSelector)));
                        else
                            queryContainer = Query<object>.Terms(_termsSelector);
                    }
                    else
                    {
                        Func<TermQueryDescriptor<object>, ITermQuery> _termSelector = t => t.Field(field).Value(value);

                        if (denyCondition)
                            queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Term(_termSelector)));
                        else
                            queryContainer = Query<object>.Term(_termSelector);
                    }
                    break;
                case ExpressionType.NotEqual:

                    Func<TermQueryDescriptor<object>, ITermQuery> _notEqualTermSelector = t => t.Field(field).Value(value);

                    if (denyCondition)                    
                        queryContainer = Query<object>.Term(_notEqualTermSelector);                    
                    else                    
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Term(_notEqualTermSelector)));
                    break;
                case ExpressionType.LessThan:
                    //TODO cast only when is necessary to double / decimal
                    Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> _lessThanSelector = r => r.Field(field).LessThan((double?)((decimal?)value));
                    if (denyCondition)                    
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Range(_lessThanSelector)));                    
                    else                    
                        queryContainer = Query<object>.Range(_lessThanSelector);
                    break;
                case ExpressionType.LessThanOrEqual:

                    Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> _lessThanOrEqualSelector = r => r.Field(field).LessThanOrEquals((double?)((decimal?)value));
                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Range(_lessThanOrEqualSelector)));
                    else
                        queryContainer = Query<object>.Range(_lessThanOrEqualSelector);
                    break;
                case ExpressionType.GreaterThan:

                    Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> _greaterThanSelector = r => r.Field(field).GreaterThan((double?)((decimal?)value));
                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Range(_greaterThanSelector)));
                    else
                        queryContainer = Query<object>.Range(_greaterThanSelector);

                    break;
                case ExpressionType.GreaterThanOrEqual:

                    Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> _greaterThanOrEqualSelector = r => r.Field(field).GreaterThanOrEquals((double?)((decimal?)value));
                    if (denyCondition)
                        queryContainer = Query<object>.Bool(b => b.MustNot(m => m.Range(_greaterThanOrEqualSelector)));
                    else
                        queryContainer = Query<object>.Range(_greaterThanOrEqualSelector);
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

        private void SetOrderBy()
        {
            if (fieldsOrderBy.Any())
            {
                fieldsOrderBy.Reverse();
                var _sortList = new List<ISort>();
                foreach (var item in fieldsOrderBy)
                {
                    _sortList.Add(new FieldSort()
                    {
                        Field = item.Field,
                        Order = item.Order
                    });
                }

                _searchRequest.Sort = _sortList;
            }
        }

        internal QueryTranslateResult Translate(Expression expression, Type elementType)
        {
            this.elementType = elementType;
            var queryMap = ElasticQueryMapper.GetMap(this.elementType);
            var _index = Indices.Index(queryMap.Index);

            _searchRequest = new SearchRequest(_index);

            if (expression is ConstantExpression == false)
                this.Visit(expression);

            _searchRequest.Human = true;
            if (_searchRequest.Query == null)
                _searchRequest.Query = Query<object>.MatchAll();

            SetOrderBy();

            if (returnNumberOfRows)
            {
                //_searchRequest.
            }

            var result = new QueryTranslateResult(_searchRequest, fieldsGroupBy, aggregations, returnNumberOfRows);

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

                    if (m.Arguments.Count == 1)
                    {
                        if (m.Arguments[0] is MethodCallExpression)
                        {
                            Visit(m.Arguments[0]);
                        }
                        _searchRequest.From = 0;
                        _searchRequest.Size = 0;
                        returnNumberOfRows = true;
                    }
                    else
                    {
                        var countAggLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                        if (countAggLambda.Body is MemberExpression)
                        {
                            var memberExp = countAggLambda.Body as MemberExpression;
                            aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                        }
                        SetAggregation();
                    }
                    return m;

                    break;
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

                    var memberContainsExp = (m.Object as MemberExpression);

                    if (memberContainsExp != null)
                    {
                        if (memberContainsExp.Expression is ConstantExpression)
                        {
                            var constMemberExp = memberContainsExp.Expression as ConstantExpression;
                            var constMemberLambdaExp = Expression.Lambda(constMemberExp).Compile();
                            var constMemberExpValue = constMemberLambdaExp.DynamicInvoke();
                            var resultConstMemberExpValue = constMemberExpValue.GetType().GetField(memberContainsExp.Member.Name).GetValue(constMemberExpValue);

                            if (typeof(System.Collections.IEnumerable).IsAssignableFrom(resultConstMemberExpValue.GetType()))
                            {
                                value = resultConstMemberExpValue;//Convert.ChangeType(resultConstMemberExpValue, resultConstMemberExpValue.GetType());
                                field = (m.Arguments.First() as MemberExpression)?.Member.Name.ToCamelCase();
                                binaryExpType = ExpressionType.Equal;//To make a terms query
                                SetQuery();
                            }
                            else
                            {
                                field = memberContainsExp.Member.Name.ToCamelCase();
                                if (m.Arguments[0] is ConstantExpression)
                                    value = (m.Arguments[0] as ConstantExpression).Value;
                                SetTextSearch();
                            }
                        }
                        else
                        {
                            field = memberContainsExp.Member.Name.ToCamelCase();
                            if (m.Arguments[0] is ConstantExpression)
                                value = (m.Arguments[0] as ConstantExpression).Value;
                            SetTextSearch();
                        }
                    }
                    return m;
                case "StartsWith":
                case "EndsWith":
                    operacao = m.Method.Name;
                    field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                        value = (m.Arguments[0] as ConstantExpression).Value;

                    SetTextSearch();

                    return m;
                    break;
                case "MatchPhrase":
                    operacao = m.Method.Name;
                    field = (m.Arguments[0] as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[1] is ConstantExpression)
                        value = (m.Arguments[1] as ConstantExpression).Value;

                    SetTextSearch();

                    return m;
                    break;
                case "Exists":

                    var mExpExists = ((ExpressionHelper.StripQuotes(m.Arguments[1]) as LambdaExpression).Body as MemberExpression).Member.Name.ToCamelCase();
                    queryContainer = Query<object>.Exists(f => f.Field(mExpExists));

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
                    return m;
                    break;
                case "MultiMatch":
                    operacao = m.Method.Name;

                    var fields = (m.Arguments[2] as NewArrayExpression);

                    foreach (var item in fields.Expressions)
                    {
                        var itemMbExp = (ExpressionHelper.StripQuotes(item) as LambdaExpression).Body as MemberExpression;
                        if (string.IsNullOrWhiteSpace(field))
                        {
                            field = itemMbExp.Member.Name.ToCamelCase();
                        }
                        else
                        {
                            field = field + ";" + itemMbExp.Member.Name.ToCamelCase();
                        }
                    }

                    if (m.Arguments[1] is ConstantExpression)
                        value = (m.Arguments[1] as ConstantExpression).Value;

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
                case "Take":

                    int? _take = null;
                    if (m.Arguments.Last() is ConstantExpression)
                    {
                        var constExp = m.Arguments.Last() as ConstantExpression;
                        _take = constExp.Value as int?;
                    }

                    _searchRequest.Size = _take;

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                    break;
                case "Skip":

                    int? _from = null;
                    if (m.Arguments.Last() is ConstantExpression)
                    {
                        var constExp = m.Arguments.Last() as ConstantExpression;
                        _from = constExp.Value as int?;
                    }

                    _searchRequest.From = _from;

                    if (m.Arguments.First() is ConstantExpression == false)
                        Visit(m.Arguments.First());

                    return m;
                    break;
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":

                    var orderLambdaExp = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments.Last());
                    if (orderLambdaExp.Body is MemberExpression)
                    {
                        var mExp = orderLambdaExp.Body as MemberExpression;

                        fieldsOrderBy.Add(new OrderBy(mExp.Member.Name.ToCamelCase(), (m.Method.Name == "OrderBy" || m.Method.Name == "ThenBy") ? SortOrder.Ascending : SortOrder.Descending));
                    }

                    if (m.Arguments.First() is ConstantExpression == false)
                        Visit(m.Arguments.First());

                    return m;
                    break;

                default:
                    throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
                    break;
            }
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            denyCondition = false;
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    {
                        denyCondition = true;
                        this.Visit(u.Operand);
                    }
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            if (b.Left.ToString().Count(s => s == '.') > 1)
            {
                bool firstV = false;
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                foreach (var token in b.Left.ToString().Split('.'))
                {
                    if (!firstV)
                    {
                        firstV = true;
                        continue;
                    }
                    sb.Append($"{token[0].ToString().ToLower()}{token.Substring(1)}.");
                  
                }
                this.field = sb.ToString().Substring(0, sb.Length - 1);
            }
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
        public QueryTranslateResult(SearchRequest searchRequest, IEnumerable<GroupBy> groupBy, IEnumerable<Aggregation> aggregation, bool returnNumberOfRows)
        {
            SearchRequest = searchRequest;
            GroupBy = groupBy;
            Aggregation = aggregation;
            ReturnNumberOfRows = returnNumberOfRows;
        }

        public SearchRequest SearchRequest { get; private set; }

        public IEnumerable<GroupBy> GroupBy { get; private set; }

        public IEnumerable<Aggregation> Aggregation { get; private set; }

        public bool ReturnNumberOfRows { get; private set; }
    }

    internal class OrderBy
    {
        public OrderBy(string field, SortOrder order)
        {
            Field = field;
            Order = order;
        }

        public string Field { get; set; }
        public SortOrder Order { get; set; }

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
