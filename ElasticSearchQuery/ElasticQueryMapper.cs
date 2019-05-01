using ElasticSearchQuery.Extensions;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ElasticSearchQuery
{
    public class ElasticQueryMapper
    {
        public static void Map(Type type, string index ,params string[] indexTypes)
        {
            if (TypeMaps.ContainsKey(type))
                throw new InvalidOperationException($"The type {type.FullName} already has a map.");

            TypeMaps.Add(type, new ElasticIndexMap(index, indexTypes));
        }

        /// <summary>
        /// Return the map for type, if not found will return a map with Type.Name in camelcase for index name and indexType and use the  ElasticsearchTypeAttribute
        /// </summary>
        /// <param name="type"></param>
        /// <returns>The Map for type<see cref="ElasticIndexMap"/></returns>
        public static ElasticIndexMap GetMap(Type type)
        {
            ElasticIndexMap map = null;
            if (TypeMaps.TryGetValue(type,out map))
               return map;
            else
            {                
                var typeNames = new List<string>();

                typeNames.Add(type.Name.ToLower());

                var attrs = type.GetCustomAttributes()
                    .Where(w => w is ElasticsearchTypeAttribute)
                    .Select(s => s as ElasticsearchTypeAttribute);

                if (attrs.Any())                
                    typeNames.AddRange(attrs.Select(s => s.Name.ToLower()));
                

                return new ElasticIndexMap(type.Name.ToLower(), typeNames.Distinct().ToArray());
            };      
               
        }

        public static void Clean()
        {
            TypeMaps.Clear();
        }

       private static IDictionary<Type, ElasticIndexMap> TypeMaps { get; set; } = new Dictionary<Type, ElasticIndexMap>();
    }


    public class ElasticIndexMap
    {
        public ElasticIndexMap(string index, string[] indexTypes)
        {
            Index = index;
            IndexTypes = indexTypes;
        }

        public string Index { get;private set; }
        public string[] IndexTypes { get; private set; }
    }
}
