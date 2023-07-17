using ElasticsearchQuery.Extensions;
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
        bool denyCondition = false;
        bool isNestedCondition = false;


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

        internal object Value => value;

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

                    Func<MatchQueryDescriptor<object>, IMatchQuery> matchSelector = f => f.Field(field).Query(value.ToString());

                    queryContainer = denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Match(matchSelector)))
                        : Query<object>.Match(matchSelector);
                    break;
                case "StartsWith":

                    Func<PrefixQueryDescriptor<object>, IPrefixQuery> startsWithSelector = f => f.Field(field).Value(value.ToString());

                    queryContainer = denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Prefix(startsWithSelector)))
                        : Query<object>.Prefix(startsWithSelector);
                    break;
                case "EndsWith":

                    Func<RegexpQueryDescriptor<object>, IRegexpQuery> endsWithSelector = f => f.Field(field).Value(".*" + value.ToString());

                    queryContainer = denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.Regexp(endsWithSelector)))
                        : Query<object>.Regexp(endsWithSelector);
                    break;
                case "MatchPhrase":

                    Func<MatchPhraseQueryDescriptor<object>, IMatchPhraseQuery> matchPhraseSelector = f => f.Field(field).Query(value.ToString());

                    queryContainer = denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.MatchPhrase(matchPhraseSelector)))
                        : Query<object>.MatchPhrase(matchPhraseSelector);
                    break;
                case "MultiMatch":
                    var fields = field.Split(';');

                    Func<MultiMatchQueryDescriptor<object>, IMultiMatchQuery> multiMachSelector = f => f.Fields(fields).Query(value.ToString());

                    queryContainer = denyCondition
                        ? Query<object>.Bool(b => b.MustNot(m => m.MultiMatch(multiMachSelector)))
                        : Query<object>.MultiMatch(multiMachSelector);

                    break;
                default:
                    break;
            }
        }



        private QueryContainer SetQuery()
        {
            switch (binaryExpType)
            {
                case ExpressionType.Equal:
                    if (typeof(System.Collections.IEnumerable).IsAssignableFrom(value.GetType()) && value.GetType().Name != "String")
                    {
                        var tempCollection = value as System.Collections.IEnumerable;

                        ITermsQuery termsSelector(TermsQueryDescriptor<object> t) => t.Field(field).Terms(tempCollection);

                        queryContainer = denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Terms(termsSelector))) : Query<object>.Terms(termsSelector);
                    }
                    else
                    {
                        ITermQuery termSelector(TermQueryDescriptor<object> t) => t.Field(field).Value(value);

                        queryContainer = denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Term(termSelector))) : Query<object>.Term(termSelector);
                    }
                    break;
                case ExpressionType.NotEqual:

                    Func<TermQueryDescriptor<object>, ITermQuery> notEqualTermSelector = t => t.Field(field).Value(value);

                    queryContainer = denyCondition
                        ? Query<object>.Term(notEqualTermSelector)
                        : Query<object>.Bool(b => b.MustNot(m => m.Term(notEqualTermSelector)));
                    break;
                case ExpressionType.LessThan:
                    // TODO cast only when is necessary to double / decimal
                    if (value.GetType().Name == nameof(DateTime))
                    {
                        IDateRangeQuery lessThan(DateRangeQueryDescriptor<object> r) => r.Field(field).LessThan((DateTime?)Convert.ToDateTime(value));
                        queryContainer = denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(lessThan))) : Query<object>.DateRange(lessThan);
                    }
                    else
                    {
                        INumericRangeQuery lessThan(NumericRangeQueryDescriptor<object> r) => r.Field(field).LessThan((double?)Convert.ToDecimal(value));
                        queryContainer = denyCondition ? Query<object>.Bool(b => b.MustNot(m => m.Range(lessThan))) : Query<object>.Range(lessThan);
                    }

                    break;
                case ExpressionType.LessThanOrEqual:

                    if (value.GetType().Name == nameof(DateTime))
                    {
                        IDateRangeQuery lessThanOrEqual(DateRangeQueryDescriptor<object> r) => r.Field(field).LessThanOrEquals((DateTime?)Convert.ToDateTime(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(lessThanOrEqual)))
                            : Query<object>.DateRange(lessThanOrEqual);
                    }
                    else
                    {
                        INumericRangeQuery greaterThanSelector(NumericRangeQueryDescriptor<object> r) => r.Field(field).LessThanOrEquals((double?)Convert.ToDecimal(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanSelector)))
                            : Query<object>.Range(greaterThanSelector);
                    }

                    break;
                case ExpressionType.GreaterThan:
                    if (value.GetType().Name == nameof(DateTime))
                    {
                        IDateRangeQuery greaterThanSelector(DateRangeQueryDescriptor<object> r) => r.Field(field).GreaterThan((DateTime?)Convert.ToDateTime(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(greaterThanSelector)))
                            : Query<object>.DateRange(greaterThanSelector);
                    }
                    else
                    {
                        INumericRangeQuery greaterThanSelector1(NumericRangeQueryDescriptor<object> r) => r.Field(field).GreaterThan((double?)Convert.ToDecimal(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanSelector1)))
                            : Query<object>.Range(greaterThanSelector1);
                    }

                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (value.GetType().Name == nameof(DateTime))
                    {
                        IDateRangeQuery greaterThanOrEqualSelector(DateRangeQueryDescriptor<object> r) => r.Field(field).GreaterThanOrEquals((DateTime?)Convert.ToDateTime(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.DateRange(greaterThanOrEqualSelector)))
                            : Query<object>.DateRange(greaterThanOrEqualSelector);
                    }
                    else
                    {
                        INumericRangeQuery greaterThanOrEqualSelector1(NumericRangeQueryDescriptor<object> r) => r.Field(field).GreaterThanOrEquals((double?)Convert.ToDecimal(value));
                        queryContainer = denyCondition
                            ? Query<object>.Bool(b => b.MustNot(m => m.Range(greaterThanOrEqualSelector1)))
                            : Query<object>.Range(greaterThanOrEqualSelector1);
                    }

                    break;

                default:
                    break;
            }
            return queryContainer;
        }

        private void SetOrderBy()
        {
            if (fieldsOrderBy.Any())
            {
                fieldsOrderBy.Reverse();
                var sortList = new List<ISort>();
                foreach (var item in fieldsOrderBy)
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
            queryContainer = null;
            field = string.Empty;
            fieldsGroupBy = new List<GroupBy>();
            aggregations = new List<Aggregation>();
            fieldsOrderBy = new List<OrderBy>();
            returnNumberOfRows = false;
            operacao = string.Empty;
            binaryExpType = default;
            value = null;
            AndCondition = true;
            denyCondition = false;
            isNestedCondition = false;
            _aggregationBase = null;
        }

        internal QueryTranslateResult Translate(Expression expression, string indexName)
        {
            var _index = Indices.Index(indexName);

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
                    if(expression is BinaryExpression
                        || expression is MethodCallExpression)
                    {
                        _searchRequest.Query &= CreateNestQuery(expression);
                    }
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
                        
                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }
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
                        aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                    }
                    else if (aggLambda.Body.NodeType == ExpressionType.Convert)
                    {
                        var memberExp = ((UnaryExpression)aggLambda.Body).Operand as MemberExpression;
                        aggregations.Add(new Aggregation(memberExp.Member.Name.ToCamelCase(), m.Method.Name));
                    }
                    SetAggregation();

                    if (m.Arguments.First() is ConstantExpression == false)
                    {
                        Visit(m.Arguments.First());
                    }

                    return m;
                case "Contains":
                    operacao = m.Method.Name;

                    var memberContainsExp = m.Object as MemberExpression ?? m.Arguments[0] as MemberExpression;
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
                                if (m.Object == null)
                                {
                                    field = (m.Arguments[1] as MemberExpression)?.Member.Name.ToCamelCase();
                                }
                                else
                                {
                                    if (m.Arguments.First().NodeType == ExpressionType.Convert)
                                    {
                                        field = ((MemberExpression)((UnaryExpression)m.Arguments.First()).Operand).Member.Name.ToCamelCase();
                                    }
                                    else
                                        field = (m.Arguments.First() as MemberExpression)?.Member.Name.ToCamelCase();
                                }
                                binaryExpType = ExpressionType.Equal;//To make a terms query
                                SetQuery();
                            }
                            else
                            {
                                field = memberContainsExp.Member.Name.ToCamelCase();
                                if (m.Arguments[0] is ConstantExpression)
                                {
                                    value = (m.Arguments[0] as ConstantExpression).Value;
                                }

                                SetTextSearch();
                            }
                        }
                        else
                        {
                            field = memberContainsExp.Member.Name.ToCamelCase();
                            if (m.Arguments[0] is ConstantExpression)
                            {
                                value = (m.Arguments[0] as ConstantExpression).Value;
                            }

                            SetTextSearch();
                        }
                    }
                    return m;
                case "StartsWith":
                case "EndsWith":
                    operacao = m.Method.Name;
                    field = (m.Object as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[0] is ConstantExpression)
                    {
                        value = (m.Arguments[0] as ConstantExpression).Value;
                    }

                    SetTextSearch();

                    return m;
                case "MatchPhrase":
                    operacao = m.Method.Name;
                    field = (m.Arguments[0] as System.Linq.Expressions.MemberExpression).Member.Name.ToCamelCase();

                    if (m.Arguments[1] is ConstantExpression)
                    {
                        value = (m.Arguments[1] as ConstantExpression).Value;
                    }

                    SetTextSearch();

                    return m;
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
                    if (m.Arguments[0] is MethodCallExpression)
                    {
                        Visit(m.Arguments[0]);
                    }
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
                                        aggregations.Add(new Aggregation(propArgMExp.Member.Name.ToCamelCase(), arMExp.Method.Name));
                                    }
                                }
                            }
                            else if(selectLambda.Body is MemberInitExpression)
                            {
                                var bindings = (selectLambda.Body as MemberInitExpression).Bindings;
                                foreach (var binding in bindings)
                                {
                                    var argument = (binding as MemberAssignment).Expression;
                                    if (argument is MethodCallExpression)
                                    {
                                        var arMExp = argument as MethodCallExpression;
                                        if (arMExp.Arguments.Count > 1)
                                        {
                                            var propArgLambda = (LambdaExpression)ExpressionHelper.StripQuotes(arMExp.Arguments[1]);
                                            var propArgMExp = propArgLambda.Body as MemberExpression;
                                            aggregations.Add(new Aggregation(propArgMExp.Member.Name.ToCamelCase(),arMExp.Method.Name, binding.Member.Name));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Visit(m.Arguments[0]);
                    return m;
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
                default:
                    throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
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
                    if (methodCallExpression.Arguments.Count() > 1)
                    {
                        isNestedCondition = true;
                        LambdaExpression lambda = (LambdaExpression)ExpressionHelper.StripQuotes(methodCallExpression.Arguments[1]);
                        var exp = lambda.Body;
                        var query = CreateNestQuery(exp);
                        isNestedCondition = false;
                        return query;
                    }
                    else
                    {
                        return queryContainer;
                    }
                }
                else
                {
                    Visit(methodCallExpression);
                    return queryContainer;
                }
            }
            var b = (BinaryExpression)expression;
            binaryExpType = b.NodeType;

            if (isNestedCondition)
            {
                if (binaryExpType == ExpressionType.AndAlso || binaryExpType == ExpressionType.And)
                {
                    var left = ((IQueryContainer)CreateNestQuery(b.Left)).Nested.Query;
                    var right = ((IQueryContainer)CreateNestQuery(b.Right)).Nested.Query;
                    var path = field.Substring(0, field.LastIndexOf('.'));
                    var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => left & right));
                    return nestedQuery;
                }
                else if (binaryExpType == ExpressionType.Or || binaryExpType == ExpressionType.OrElse)
                {
                    var left = ((IQueryContainer)CreateNestQuery(b.Left)).Nested.Query;
                    var right = ((IQueryContainer)CreateNestQuery(b.Right)).Nested.Query;
                    var path = field.Substring(0, field.LastIndexOf('.'));
                    var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => left | right));
                    return nestedQuery;
                }
            }
            else
            {
                if (binaryExpType == ExpressionType.AndAlso || binaryExpType == ExpressionType.And)
                {
                    return CreateNestQuery(b.Left) & CreateNestQuery(b.Right);
                }
                else if (binaryExpType == ExpressionType.OrElse || binaryExpType == ExpressionType.Or)
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
                    binaryExpType = b.NodeType;
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            Visit(b.Right);
            if (isNestedCondition)
            {
                var query = SetQuery();
                var path = field.Substring(0, field.LastIndexOf('.') == -1 ? 0 : field.LastIndexOf('.'));
                var nestedQuery = Query<object>.Nested(n => n.Path(path).Query(x => query));
                return nestedQuery;
            }
            return SetQuery();
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
            string displayName = null;
            if (m.Member.CustomAttributes.ToList().FirstOrDefault() != null && m.Member.CustomAttributes.ToList().FirstOrDefault().ConstructorArguments.Count > 0 && isNestedCondition == true)
            {
                displayName = m.Member.CustomAttributes.ToList().FirstOrDefault().ConstructorArguments[0].Value.ToString();
            }

            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
                if (displayName != null && isNestedCondition == true)
                {
                    field = displayName;
                }
                else
                {
                    field = m.Member.Name.ToCamelCase();
                }
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
        public Aggregation(string field, string method, string aggName = null)
        {
            Field = field;
            Method = method;
            AggName = aggName ?? $"{Method}_{Field}";
        }

        public string Field { get; set; }
        public string Method { get; set; }

        public string AggName { get; set; }
    }
}
