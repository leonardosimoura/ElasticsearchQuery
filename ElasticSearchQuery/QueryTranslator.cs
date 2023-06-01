using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using ElasticsearchQuery.Extensions;
using ElasticsearchQuery.Helpers;
using Nest;

namespace ElasticsearchQuery
{
    public class QueryTranslator : ExpressionVisitor
    {
        private SearchRequest _searchRequest;
        private QueryContainer _queryContainer;
        private string _field = string.Empty;
        private List<GroupBy> _fieldsGroupBy = new List<GroupBy>();
        private List<Aggregation> _aggregations = new List<Aggregation>();
        private List<OrderBy> _fieldsOrderBy = new List<OrderBy>();
        private bool _returnNumberOfRows = false;
        private string _operacao = string.Empty;
        private ExpressionType _binaryExpType;
        private object _value = null;
        private bool _andCondition = true;
        private bool _denyCondition = false;
        private bool _isNestedCondition = false;
        private AggregationBase _aggregationBase;

        public QueryTranslator()
        {
        }

        public AggregationBase AggregationBase
        {
            get
            {
                return _aggregationBase;
            }

            set
            {
                if (_aggregationBase == null)
                {
                    _aggregationBase = value;
                }
                else
                {
                    _aggregationBase = _aggregationBase && value;
                }
            }
        }

        internal object Value => _value;

        public AggregationDictionary ObterAgrupamentoNest(IEnumerable<GroupBy> agrupamentos, AggregationBase aggregations = null)
        {
            foreach (var aggr in agrupamentos)
            {
                // Tipos de agrupamento
                if (aggr.PropertyType == typeof(DateTime)
                    || aggr.PropertyType == typeof(DateTime?))
                {
                    var dateHistAgg = new DateHistogramAggregation(aggr.Field)
                    {
                        Missing = (DateTime?)null,// (DateTime?)aggr.Missing,
                        Field = aggr.Field,

                        // Aggregations = ((aggregations != null) ? aggregations : null),
                        Interval = DateInterval.Day,
                        MinimumDocumentCount = 1,

                        // Script = (!string.IsNullOrWhiteSpace(aggr.Script)) ? new InlineScript(aggr.Script) : null
                    };

                    if (aggregations != null)
                    {
                        dateHistAgg.Aggregations = aggregations;
                    }

                    aggregations = dateHistAgg;
                }
                else
                {
                    var termsAgg = new TermsAggregation(aggr.Field)
                    {
                        Field = aggr.Field,

                        // Aggregations = ((aggregations != null) ? aggregations : null),
                        Size = int.MaxValue,

                        // Missing = null,//aggr.Missing,
                        MinimumDocumentCount = 1,

                        // Script = (!string.IsNullOrWhiteSpace(aggr.Script)) ? new InlineScript(aggr.Script) : null
                    };

                    if (aggregations != null)
                    {
                        termsAgg.Aggregations = aggregations;
                    }

                    aggregations = termsAgg;
                }
            }
            return aggregations;
        }

        public QueryTranslateResult Translate(Expression expression, string indexName)
        {
            _searchRequest = new SearchRequest(indexName);
            {
                if (expression is ConstantExpression == false)
                {
                    Visit(expression);
                }
            }

            _searchRequest.Human = true;
            if (_searchRequest.Query == null)
            {
                _searchRequest.Query = Query<object>.MatchAll();
            }

            SetOrderBy();

            if (_returnNumberOfRows)
            {
                // _searchRequest.
            }

            var result = new QueryTranslateResult(_searchRequest, _fieldsGroupBy, _aggregations, _returnNumberOfRows);
            ResetState();
            return result;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            switch (m.Method.Name)
            {
                case "Any":
                case "Where":

                    foreach (var argument in m.Arguments)
                    {
                        if (argument is MethodCallExpression)
                        {
                            Visit(argument);
                        }
                    }
                    LambdaExpression lambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    var expression = lambda.Body;
                    _searchRequest.Query &= CreateNestQuery(expression);

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
                        _returnNumberOfRows = true;
                    }
                    else
                    {
                        var countAggLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                        if (countAggLambda.Body is MemberExpression)
                        {
                            var memberExp = countAggLambda.Body as MemberExpression;
                            _aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                        }
                        SetAggregation();
                    }
                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                case "Min":
                case "Max":
                case "Sum":
                case "Average":
                    var aggLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    if (aggLambda.Body is MemberExpression)
                    {
                        var memberExp = aggLambda.Body as MemberExpression;
                        _aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                    }
                    else if (aggLambda.Body.NodeType == ExpressionType.Convert)
                    {
                        var memberExp = ((UnaryExpression)aggLambda.Body).Operand as MemberExpression;
                        _aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                    }
                    SetAggregation();

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                case "Contains":
                    _operacao = m.Method.Name;

                    var memberContainsExp = m.Object as MemberExpression;

                    if (memberContainsExp == null)
                    {
                        memberContainsExp = m.Arguments[0] as MemberExpression;
                    }

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
                                _value = resultConstMemberExpValue; // Convert.ChangeType(resultConstMemberExpValue, resultConstMemberExpValue.GetType());
                                if (m.Object == null)
                                {
                                    _field = (m.Arguments[1] as MemberExpression)?.Member.Name.ToCamelCase();
                                }
                                else
                                {
                                    if (m.Arguments.First().NodeType == ExpressionType.Convert)
                                    {
                                        _field = ((MemberExpression)((UnaryExpression)m.Arguments.First()).Operand).Member.Name.ToCamelCase();
                                    }
                                    else
                                        _field = (m.Arguments.First() as MemberExpression)?.Member.Name.ToCamelCase();
                                }
                                _binaryExpType = ExpressionType.Equal; // To make a terms query
                                SetQuery();
                            }
                            else
                            {
                                _field = memberContainsExp.Member.Name.ToCamelCase();
                                if (m.Arguments[0] is ConstantExpression)
                                {
                                    _value = (m.Arguments[0] as ConstantExpression).Value;
                                }

                                SetTextSearch();
                            }
                        }
                        else
                        {
                            _field = memberContainsExp.Member.Name.ToCamelCase();
                            if (m.Arguments[0] is ConstantExpression)
                            {
                                _value = (m.Arguments[0] as ConstantExpression).Value;
                            }

                            SetTextSearch();
                        }
                    }
                    return m;
                case "StartsWith":
                case "EndsWith":
                    _operacao = m.Method.Name;
                    _field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                    {
                        _value = (m.Arguments[0] as ConstantExpression).Value;
                    }

                    SetTextSearch();

                    return m;
                case "MatchPhrase":
                    _operacao = m.Method.Name;
                    _field = (m.Arguments[0] as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[1] is ConstantExpression)
                    {
                        _value = (m.Arguments[1] as ConstantExpression).Value;
                    }

                    SetTextSearch();

                    return m;
                case "Exists":

                    var mExpExists = ((ExpressionHelper.StripQuotes(m.Arguments[1]) as LambdaExpression).Body as MemberExpression).Member.Name.ToCamelCase();
                    _queryContainer = Query<object>.Exists(f => f.Field(mExpExists));

                    if (_searchRequest.Query == null)
                    {
                        _searchRequest.Query = _queryContainer;
                    }
                    else
                    {
                        if (_andCondition)
                        {
                            _searchRequest.Query = _searchRequest.Query && _searchRequest.Query;
                        }
                        else
                        {
                            _searchRequest.Query = _searchRequest.Query || _searchRequest.Query;
                        }
                    }
                    return m;
                case "MultiMatch":
                    _operacao = m.Method.Name;

                    var fields = m.Arguments[2] as NewArrayExpression;

                    foreach (var item in fields.Expressions)
                    {
                        var itemMbExp = (ExpressionHelper.StripQuotes(item) as LambdaExpression).Body as MemberExpression;
                        if (string.IsNullOrWhiteSpace(_field))
                        {
                            _field = itemMbExp.Member.Name.ToCamelCase();
                        }
                        else
                        {
                            _field = _field + ";" + itemMbExp.Member.Name.ToCamelCase();
                        }
                    }

                    if (m.Arguments[1] is ConstantExpression)
                    {
                        _value = (m.Arguments[1] as ConstantExpression).Value;
                    }

                    SetTextSearch();

                    return m;
                case "GroupBy":

                    LambdaExpression groupByLambda = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments[1]);
                    if (groupByLambda.Body is NewExpression)
                    {
                        var newExp = groupByLambda.Body as NewExpression;

                        foreach (var item in newExp.Members)
                        {
                            var pInfo = item as PropertyInfo;
                            _fieldsGroupBy.Add(new GroupBy(item.Name.ToCamelCase(), pInfo.PropertyType));
                        }
                    }
                    else if (groupByLambda.Body is MemberExpression)
                    {
                        var memberExp = groupByLambda.Body as MemberExpression;
                        _fieldsGroupBy.Add(new GroupBy(memberExp.Member.Name.ToCamelCase(), groupByLambda.Body.Type));
                    }

                    SetAggregation();

                    return m;
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
                                        _aggregations.Add(new Aggregation(propArgMExp.Member.Name.ToCamelCase(), arMExp.Method.Name));
                                    }
                                }
                            }
                        }
                    }

                    Visit(m.Arguments[0]);
                    return m;
                case "Take":

                    int? take = null;
                    if (m.Arguments.Last() is ConstantExpression)
                    {
                        var constExp = m.Arguments.Last() as ConstantExpression;
                        take = constExp.Value as int?;
                    }

                    _searchRequest.Size = take;

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                case "Skip":

                    int? from = null;
                    if (m.Arguments.Last() is ConstantExpression)
                    {
                        var constExp = m.Arguments.Last() as ConstantExpression;
                        from = constExp.Value as int?;
                    }

                    _searchRequest.From = from;

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":

                    var orderLambdaExp = (LambdaExpression)ExpressionHelper.StripQuotes(m.Arguments.Last());
                    if (orderLambdaExp.Body is MemberExpression)
                    {
                        var mExp = orderLambdaExp.Body as MemberExpression;

                        _fieldsOrderBy.Add(new OrderBy(mExp.Member.Name.ToCamelCase(), (m.Method.Name == "OrderBy" || m.Method.Name == "ThenBy") ? SortOrder.Ascending : SortOrder.Descending));
                    }

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                default:
                    throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
            }
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            _denyCondition = false;
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    {
                        _denyCondition = true;
                        Visit(u.Operand);
                    }
                    break;
                case ExpressionType.Quote:
                    {
                        Visit(u.Operand);
                    }
                    break;
                case ExpressionType.Convert:
                    {
                        Visit(u.Operand);
                    }
                    break;

                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected QueryContainer CreateNestQuery(Expression expression)
        {
            if (expression is MethodCallExpression methodCallExpression)
            {
                if (methodCallExpression.Method.Name == "Any")
                {
                    if(methodCallExpression.Arguments.Count() > 1)
                    {
                        _isNestedCondition = true;
                        LambdaExpression lambda = (LambdaExpression)ExpressionHelper.StripQuotes(methodCallExpression.Arguments[1]);
                        var exp = lambda.Body;
                        var query = CreateNestQuery(exp);
                        _isNestedCondition = false;
                        return query;
                    }
                    else
                    {
                        return _queryContainer;
                    }
                }
                else
                {
                    Visit(methodCallExpression);
                    return _queryContainer;
                }
            }
            var b = (BinaryExpression)expression;
            _binaryExpType = b.NodeType;

            if (_isNestedCondition)
            {
                if (_binaryExpType == ExpressionType.AndAlso || _binaryExpType == ExpressionType.And)
                {
                    var left = ((IQueryContainer)CreateNestQuery(b.Left)).Nested.Query;
                    var right = ((IQueryContainer)CreateNestQuery(b.Right)).Nested.Query;
                    var path = _field.Substring(0, _field.LastIndexOf('.'));
                    var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => left & right));
                    return nestedQuery;
                }
                else if (_binaryExpType == ExpressionType.Or || _binaryExpType == ExpressionType.OrElse)
                {
                    var left = ((IQueryContainer)CreateNestQuery(b.Left)).Nested.Query;
                    var right = ((IQueryContainer)CreateNestQuery(b.Right)).Nested.Query;
                    var path = _field.Substring(0, _field.LastIndexOf('.'));
                    var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => left | right));
                    return nestedQuery;
                }
            }
            else
            {
                if (_binaryExpType == ExpressionType.AndAlso || _binaryExpType == ExpressionType.And)
                {
                    return CreateNestQuery(b.Left) & CreateNestQuery(b.Right);
                }
                else if (_binaryExpType == ExpressionType.OrElse || _binaryExpType == ExpressionType.Or)
                {
                    return CreateNestQuery(b.Left) | CreateNestQuery(b.Right);
                }
            }
            Visit(b.Left);

            switch (b.NodeType)
            {
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    _binaryExpType = b.NodeType;
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            Visit(b.Right);
            if (_isNestedCondition)
            {
                var query = SetQuery();
                var path = _field.Substring(0, _field.LastIndexOf('.') == -1 ? 0 : _field.LastIndexOf('.'));
                var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => query));
                return nestedQuery;
            }
            return SetQuery();
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Value == null)
            {
                _queryContainer = Query<object>.Term(f => f.Field(_field).Value(null));
            }
            else
            {
                switch (Type.GetTypeCode(c.Value.GetType()))
                {
                    case TypeCode.Object:
                        throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", c.Value));
                    default:
                        _value = c.Value;
                        break;
                }
            }

            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            string displayName = null;
            if (m.Member.CustomAttributes.ToList().FirstOrDefault() != null && m.Member.CustomAttributes.ToList().FirstOrDefault().ConstructorArguments.Count > 0 && _isNestedCondition == true)
            {
                displayName = m.Member.CustomAttributes.ToList().FirstOrDefault().ConstructorArguments[0].Value.ToString();
            }

            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
                if (displayName != null && _isNestedCondition == true)
                {
                    _field = displayName;
                }
                else
                {
                    _field = m.Member.Name.ToCamelCase();
                }
            }
            else if (m.Expression != null && m.Expression.NodeType == ExpressionType.Constant)
            {
                _value = (m.Member as FieldInfo).GetValue(((m as MemberExpression).Expression as ConstantExpression).Value);
            }
            else if (m.Expression == null)
            {
                var field = m.Type.GetField(m.Member.Name);
                _value = field.GetValue(null);
            }
            return m;
        }

        private void SetAggregation()
        {
            foreach (var item in _aggregations)
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

            _searchRequest.Aggregations = ObterAgrupamentoNest(_fieldsGroupBy, AggregationBase);
        }

        private void SetTextSearch()
        {
            switch (_operacao)
            {
                case "Contains":

                    Func<MatchQueryDescriptor<object>, IMatchQuery> matchSelector = f => f.Field(_field).Query(_value.ToString());

                    _queryContainer = _denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Match(matchSelector)))
                        : Query<object>.Match(matchSelector);
                    break;
                case "StartsWith":

                    Func<PrefixQueryDescriptor<object>, IPrefixQuery> startsWithSelector = f => f.Field(_field).Value(_value.ToString());

                    _queryContainer = _denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Prefix(startsWithSelector)))
                        : Query<object>.Prefix(startsWithSelector);
                    break;
                case "EndsWith":

                    Func<RegexpQueryDescriptor<object>, IRegexpQuery> endsWithSelector = f => f.Field(_field).Value(".*" + _value.ToString());

                    _queryContainer = _denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Regexp(endsWithSelector)))
                        : Query<object>.Regexp(endsWithSelector);
                    break;
                case "MatchPhrase":

                    Func<MatchPhraseQueryDescriptor<object>, IMatchPhraseQuery> matchPhraseSelector = f => f.Field(_field).Query(_value.ToString());

                    _queryContainer = _denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.MatchPhrase(matchPhraseSelector)))
                        : Query<object>.MatchPhrase(matchPhraseSelector);
                    break;
                case "MultiMatch":
                    var fields = _field.Split(';');

                    Func<MultiMatchQueryDescriptor<object>, IMultiMatchQuery> multiMachSelector = f => f.Fields(fields).Query(_value.ToString());

                    _queryContainer = _denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.MultiMatch(multiMachSelector)))
                        : Query<object>.MultiMatch(multiMachSelector);

                    break;
                default:
                    break;
            }
        }

        private QueryContainer SetQuery()
        {
            switch (_binaryExpType)
            {
                case ExpressionType.Equal:
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(_value.GetType()) && _value.GetType().Name != "String")
                    {
                        var tempCollection = _value as System.Collections.IEnumerable;

                        Func<TermsQueryDescriptor<object>, ITermsQuery> termsSelector = t => t.Field(_field).Terms(tempCollection);

                        _queryContainer = _denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Terms(termsSelector))) : Query<object>.Terms(termsSelector);
                    }
                    else
                    {
                        Func<TermQueryDescriptor<object>, ITermQuery> termSelector = t => t.Field(_field).Value(_value);

                        _queryContainer = _denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Term(termSelector))) : Query<object>.Term(termSelector);
                    }
                    break;
                case ExpressionType.NotEqual:

                    Func<TermQueryDescriptor<object>, ITermQuery> notEqualTermSelector = t => t.Field(_field).Value(_value);

                    _queryContainer = _denyCondition
                        ? Query<object>.Term(notEqualTermSelector)
                        : Query<object>.Bool(b => b.MustNot(m => m.Term(notEqualTermSelector)));
                    break;
                case ExpressionType.LessThan:
                    // TODO cast only when is necessary to double / decimal
                    if (_value.GetType().Name == nameof(DateTime))
                    {
                        Func<DateRangeQueryDescriptor<object>, IDateRangeQuery> lessThan = r => r.Field(_field).LessThan((DateTime?)Convert.ToDateTime(_value));
                        _queryContainer = _denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(lessThan))) : Query<object>.DateRange(lessThan);
                    }
                    else
                    {
                        Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> lessThan = r => r.Field(_field).LessThan((double?)Convert.ToDecimal(_value));
                        _queryContainer = _denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Range(lessThan))) : Query<object>.Range(lessThan);
                    }

                    break;
                case ExpressionType.LessThanOrEqual:

                    if (_value.GetType().Name == nameof(DateTime))
                    {
                        Func<DateRangeQueryDescriptor<object>, IDateRangeQuery> lessThanOrEqual = r => r.Field(_field).LessThanOrEquals((DateTime?)Convert.ToDateTime(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(lessThanOrEqual)))
                            : Query<object>.DateRange(lessThanOrEqual);
                    }
                    else
                    {
                        Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> greaterThanSelector = r => r.Field(_field).LessThanOrEquals((double?)Convert.ToDecimal(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanSelector)))
                            : Query<object>.Range(greaterThanSelector);
                    }

                    break;
                case ExpressionType.GreaterThan:
                    if (_value.GetType().Name == nameof(DateTime))
                    {
                        Func<DateRangeQueryDescriptor<object>, IDateRangeQuery> greaterThanSelector = r => r.Field(_field).GreaterThan((DateTime?)Convert.ToDateTime(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(greaterThanSelector)))
                            : Query<object>.DateRange(greaterThanSelector);
                    }
                    else
                    {
                        Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> greaterThanSelector1 = r => r.Field(_field).GreaterThan((double?)Convert.ToDecimal(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanSelector1)))
                            : Query<object>.Range(greaterThanSelector1);
                    }

                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (_value.GetType().Name == nameof(DateTime))
                    {
                        Func<DateRangeQueryDescriptor<object>, IDateRangeQuery> greaterThanOrEqualSelector = r => r.Field(_field).GreaterThanOrEquals((DateTime?)Convert.ToDateTime(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(greaterThanOrEqualSelector)))
                            : Query<object>.DateRange(greaterThanOrEqualSelector);
                    }
                    else
                    {
                        Func<NumericRangeQueryDescriptor<object>, INumericRangeQuery> greaterThanOrEqualSelector1 = r => r.Field(_field).GreaterThanOrEquals((double?)Convert.ToDecimal(_value));
                        _queryContainer = _denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanOrEqualSelector1)))
                            : Query<object>.Range(greaterThanOrEqualSelector1);
                    }

                    break;

                default:
                    break;
            }
            return _queryContainer;
        }

        private void SetOrderBy()
        {
            if (_fieldsOrderBy.Any())
            {
                _fieldsOrderBy.Reverse();
                var sortList = new List<ISort>();
                foreach (var item in _fieldsOrderBy)
                {
                    sortList.Add(new FieldSort()
                    {
                        Field = item.Field,
                        Order = item.Order
                    });
                }

                _searchRequest.Sort = sortList;
            }
        }

        private void ResetState()
        {
            _searchRequest = null;
            _queryContainer = null;
            _field = string.Empty;
            _fieldsGroupBy = new List<GroupBy>();
            _aggregations = new List<Aggregation>();
            _fieldsOrderBy = new List<OrderBy>();
            _returnNumberOfRows = false;
            _operacao = string.Empty;
            _binaryExpType = default(ExpressionType);
            _value = null;
            _andCondition = true;
            _denyCondition = false;
            _isNestedCondition = false;
            _aggregationBase = null;
        }
    }

    public class QueryTranslateResult
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

    public class GroupBy
    {
        public GroupBy(string field, Type type)
        {
            Field = field;
            PropertyType = type;
        }

        public string Field { get; set; }

        public Type PropertyType { get; set; }
    }

    public class Aggregation
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

    public class OrderBy
    {
        public OrderBy(string field, SortOrder order)
        {
            Field = field;
            Order = order;
        }

        public string Field { get; set; }

        public SortOrder Order { get; set; }
    }
}