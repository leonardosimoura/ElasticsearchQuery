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
    internal class ElasticQueryProvider : QueryProvider
    {
        private readonly IElasticClient _elasticClient;
        public ElasticQueryProvider(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }
        public override object Execute(Expression expression)
        {
            //Element type of IQueryable
            //Need this for the elastic search request
            Type elementType = TypeSystem.GetElementType(expression.Type);
            Type expType = TypeSystem.GetElementType(expression.Type);

            if (Attribute.IsDefined(expType, typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), false)
                && expType.IsGenericType && expType.Name.Contains("AnonymousType")
                && (expType.Name.StartsWith("<>") || expType.Name.StartsWith("VB$"))
                && (expType.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic)
            {
                var mExp = expression as MethodCallExpression;
                var t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type) ;

                while (t1.IsGenericType)
                {
                    mExp = mExp.Arguments[0] as MethodCallExpression;
                    t1 = TypeSystem.GetElementType(mExp.Arguments[0].Type);
                }

                elementType = t1;
            }

            //Whem use IQueryable.Sum() for example need make this to get the elementType
            if (!elementType.IsClass)
            {
                var exp = expression as MethodCallExpression;
                elementType = exp.Arguments[0].Type.GenericTypeArguments[0];
            }

            var elasticQueryResult = new QueryTranslator().Translate(expression, elementType);

            var method = typeof(ElasticClient)
                           .GetMethods()
                          .Where(m => m.Name == "Search")
                          .Select(m => new
                          {
                              Method = m,
                              Params = m.GetParameters(),
                              Args = m.GetGenericArguments()
                          })
                        .Where(x => x.Params.Length == 1
                                    && x.Args.Length == 1
                                    && x.Params.First().ParameterType == typeof(ISearchRequest))
                        .Select(x => x.Method).First();

            MethodInfo generic = method.MakeGenericMethod(elementType);
            var request = generic.Invoke(_elasticClient, new object[] { elasticQueryResult.SearchRequest });
            var closedGeneric = typeof(SearchResponse<>).MakeGenericType(elementType);           

            //Simple Agregation
            if (!expType.IsClass)
            {
                var prop = closedGeneric.GetProperty("Aggregations");
                var aggs = prop.GetValue(request) as AggregateDictionary;

                if (aggs.Any())
                {
                    var valueAgg = aggs.First().Value as ValueAggregate;
                    return ConveterToType(expType, valueAgg.Value);
                }
                return ConveterToType(expType, 0);
            }
            else
            {
                
                if (elasticQueryResult.GroupBy.Any() || elasticQueryResult.Aggregation.Any())
                {
                    var mExp = expression as MethodCallExpression;
                    var lastLambExp = ExpressionHelper.StripQuotes(mExp.Arguments.Last()) as LambdaExpression;
                  
                    var prop = closedGeneric.GetProperty("Aggregations");
                    var aggs = prop.GetValue(request) as AggregateDictionary;

                    var resultAgg = ConvertAggregateResult(aggs);

                    if (lastLambExp.Body is NewExpression)
                    {
                        var newExp = lastLambExp.Body as NewExpression;

                        //A set of items
                        if (expression.Type.GetInterfaces().Any(t => t == typeof(IQueryable) || t == typeof(IQueryable<>)  ))
                        {
                            var typeList = typeof(List<>).MakeGenericType(expType);
                            var list = Activator.CreateInstance(typeList);

                            if (!resultAgg.Any())
                                return list;

                            var addMethod = typeList.GetMethod("Add");

                            var membersExps = newExp.Arguments.Where(s => (s is MemberExpression))
                                                            .Select(s => s as MemberExpression)
                                                            .Select(s => s.Member.Name.ToCamelCase());                           

                            var methodCallsExps = newExp.Arguments.Where(s => (s is MethodCallExpression)).Select(s => s as MethodCallExpression) .Select(s => s.Method.Name + "_" + ((s.Arguments.Last() as LambdaExpression ).Body as MemberExpression).Member.Name.ToCamelCase());


                            Func<string, bool> filterPredicate = (string str) =>
                            {
                                return membersExps.Contains(str) || methodCallsExps.Contains(str);
                            };

                            if (membersExps.Count() == 1 && membersExps.First() == "key")
                            {
                                filterPredicate = (string str) =>
                                {
                                    return membersExps.Contains(str) || methodCallsExps.Contains(str);
                                };
                            }

                            var parametersInfo = newExp.Constructor.GetParameters();

                            foreach (var item in resultAgg)
                            {      
                                var parameters = item.Where(w => membersExps.Contains(w.Key) || methodCallsExps.Contains(w.Key)).Select(s => s.Value).ToArray();

                                
                                if (membersExps.Count() == 1 && membersExps.First() == "key")
                                {
                                    parameters = new object[] { item.First().Value }.Concat(parameters).ToArray();
                                }

                                for (int i = 0; i < parameters.Count(); i++)
                                {
                                    parameters[i] = ConveterToType(parametersInfo.ElementAt(i).ParameterType, parameters[i]);
                                }


                                var obj = newExp.Constructor.Invoke(parameters.ToArray());
                                addMethod.Invoke(list, new object[] { obj });
                            }

                            return list;
                        }
                        else
                        {
                            //Just one item

                            if (!resultAgg.Any())
                                return null;
                            
                            var obj = newExp.Constructor.Invoke(resultAgg.First().Select(s => s.Value).ToArray());
                            return obj;
                        }                                                
                    }

                    throw new NotImplementedException("Need implement the anonymous projection on select");
                }
                else
                {
                    var prop = closedGeneric.GetProperty("Documents");

                    if (prop.PropertyType.GetGenericArguments().Count() == 1 &&
                        prop.PropertyType.GetGenericArguments() .First() == expType)
                    {
                        var value = prop.GetValue(request);
                        return value;
                    }
                    else
                    {
                        throw new NotImplementedException("Need implement the anonymous projection on select");
                    }
                }               
            }            
        }

        private object ConveterToType(Type type, object value)
        {
            if (type == typeof(decimal))
            {
                return Convert.ToDecimal(value);
            }
            else if (type == typeof(decimal?))
            {
                if (value == null)
                    return null;
                return Convert.ToDecimal(value);
            }
            else if (type == typeof(double))
            {
                return Convert.ToDouble(value);
            }
            else if (type == typeof(double?))
            {
                if (value == null)
                    return null;
                return Convert.ToDouble(value);
            }
            else if (type == typeof(int))
            {
                return Convert.ToInt32(value);
            }
            else if (type == typeof(int?))
            {
                if (value == null)
                    return null;
                return Convert.ToInt32(value);
            }
            else if (type == typeof(long))
            {
                return Convert.ToInt64(value);
            }
            else if (type == typeof(long?))
            {
                if (value == null)
                    return null;
                return Convert.ToInt64(value);
            }
            return Convert.ChangeType(value, type);
        }


        /// <summary>
        /// Monta uma List<Dictionary<string, object>> onde tem nome da aggregação e o valor 
        /// </summary>
        /// <param name="aggregates">Aggregates do retorno do elastic</param>
        /// <param name="props">passe nulo na primeira chamada</param>
        /// <returns></returns>
        public List<Dictionary<string, object>> ConvertAggregateResult(IReadOnlyDictionary<string, IAggregate> aggregates, Dictionary<string, object> props = null)
        {
            
            var list = new List<Dictionary<string, object>>();
            if (props == null)
                props = new Dictionary<string, object>();

            var add = false;
            foreach (var item in aggregates)
            {
                //Se o item for um bucket
                if (item.Value is BucketAggregate && !item.Key.EndsWith("Count"))
                {
                    var bucket = item.Value as BucketAggregate;
                    //Para receber as Aggregadas do item
                    IReadOnlyDictionary<string, IAggregate> _subAggregates = null;

                    foreach (var itemBucket in bucket.Items)
                    {
                        if (props.Any(w => w.Key == item.Key))
                            props.Remove(item.Key);

                        //Caso seja usado um novo tipo adicionar outra condição

                        //Usado no TermsAggregation
                        if (itemBucket is KeyedBucket<object>)
                        {
                            var _temp = itemBucket as KeyedBucket<object>;
                            props.Add(item.Key, _temp.Key);
                            //_subAggregates = _temp.Aggregations;

                            var t = (from a in _temp.Keys
                                     join b in _temp.ToList() on a equals b.Key
                                     select new { Key = a, Value = b.Value });
                            var _dc = new Dictionary<string, IAggregate>();
                            foreach (var _item in t)
                            {
                                _dc.Add(_item.Key, _item.Value);
                            }

                            _subAggregates = new System.Collections.ObjectModel.ReadOnlyDictionary<string, IAggregate>(_dc);
                        }
                        else if (itemBucket is RangeBucket)
                        {
                            var _temp = itemBucket as RangeBucket;
                            props.Add(item.Key, _temp.Key);
                            //_subAggregates = _temp.Aggregations;
                            var t = (from a in _temp.Keys
                                     join b in _temp.ToList() on a equals b.Key
                                     select new { Key = a, Value = b.Value });
                            var _dc = new Dictionary<string, IAggregate>();
                            foreach (var _item in t)
                            {
                                _dc.Add(_item.Key, _item.Value);
                            }

                            _subAggregates = new System.Collections.ObjectModel.ReadOnlyDictionary<string, IAggregate>(_dc);
                        }
                        //Usado no DateHistogramAggregation
                        else if (itemBucket is DateHistogramBucket)
                        {
                            var _temp = itemBucket as DateHistogramBucket;
                            props.Add(item.Key, _temp.Date);
                            //_subAggregates = _temp.Aggregations;
                            var t = (from a in _temp.Keys
                                     join b in _temp.ToList() on a equals b.Key
                                     select new { Key = a, Value = b.Value });
                            var _dc = new Dictionary<string, IAggregate>();
                            foreach (var _item in t)
                            {
                                _dc.Add(_item.Key, _item.Value);
                            }

                            _subAggregates = new System.Collections.ObjectModel.ReadOnlyDictionary<string, IAggregate>(_dc);
                        }

                        list.AddRange(ConvertAggregateResult(_subAggregates, props));
                    }
                }
                //Quando ele for um Value considera sendo o ultimo nivel
                else if (item.Value is ValueAggregate)
                {
                    add = true;
                    var _temp = item.Value as ValueAggregate;

                    if (props.Any(w => w.Key == item.Key))
                        props.Remove(item.Key);

                    props.Add(item.Key, _temp.Value);
                }
                else if (item.Value is BucketAggregate && item.Key.EndsWith("Count"))
                {
                    add = true;
                    var _temp = item.Value as BucketAggregate;

                    if (props.Any(w => w.Key == item.Key))
                        props.Remove(item.Key);

                    props.Add(item.Key, _temp.Items.Select(s => (s as KeyedBucket<object>).Key).Distinct().Count());
                }
            }

            //Adicionar o item na lista
            if (add)
            {
                var _temp = new Dictionary<string, object>();

                foreach (var item in props)
                    _temp.Add(item.Key, item.Value);

                list.Add(_temp);
            }

            return list;
        }
    }
}
