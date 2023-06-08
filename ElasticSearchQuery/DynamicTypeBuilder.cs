using Nest;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Dynamic.Core;

namespace ElasticsearchQuery
{
    internal static class DynamicTypeBuilder
    {
        public static IList ToList(AggregateDictionary dictionary, int count)
        {
            var properties = new List<DynamicProperty>
                    {
                        new DynamicProperty("RowCount", typeof(int))
                    };
            foreach (var key in dictionary.Keys)
            {
                properties.Add(new DynamicProperty(key, typeof(double?)));
            }
            var aggregationResponseType = DynamicClassFactory.CreateType(properties);
            var aggregationResponses = CreateListFor(aggregationResponseType);

            DynamicClass materializedAggregationResponse = Activator.CreateInstance(aggregationResponseType) as DynamicClass;
            materializedAggregationResponse.SetDynamicPropertyValue("RowCount", count);
            foreach (var key in dictionary.Keys)
            {
                if (dictionary.TryGetValue(key, out IAggregate value))
                {
                    materializedAggregationResponse.SetDynamicPropertyValue(key, ((ValueAggregate)value).Value);
                }
            }
            aggregationResponses.Add(materializedAggregationResponse);
            return aggregationResponses;
        }
        private static IList CreateListFor(Type type)
        {
            var genericType = typeof(List<>).MakeGenericType(type);

            return (IList)Activator.CreateInstance(genericType);
        }
    }
}
