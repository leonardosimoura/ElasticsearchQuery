// Licensed under the Apache 2.0 License. See LICENSE.txt in the project root for more information.

using ElasticLinq.Mapping;
using ElasticLinq.Request.Criteria;
using ElasticLinq.Utility;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;

namespace ElasticLinq.Request.Formatters
{
    /// <summary>
    /// Formats a SearchRequest into a JSON POST to be sent to Elasticsearch.
    /// </summary>
    class SearchRequestFormatter
    {
        static readonly CultureInfo transportCulture = CultureInfo.InvariantCulture;

        readonly Lazy<string> body;
       
        readonly IElasticMapping mapping;
        readonly SearchRequest searchRequest;

        /// <summary>
        /// Create a new SearchRequestFormatter for the given connection, mapping and search request.
        /// </summary>
       
        /// <param name="mapping">The IElasticMapping used to format the SearchRequest.</param>
        /// <param name="searchRequest">The SearchRequest to be formatted.</param>
        public SearchRequestFormatter(IElasticMapping mapping, SearchRequest searchRequest)
        {
            
            this.mapping = mapping;
            this.searchRequest = searchRequest;

            body = new Lazy<string>(() => CreateBody().ToString(Formatting.None));
        }

        /// <summary>
        /// The JSON formatted POST body for the request to be sent to Elasticsearch.
        /// </summary>
        public string Body => body.Value;
        public JObject FullQuery;

        /// <summary>
        /// Create the Json HTTP request body for this request given the search query and connection.
        /// </summary>
        /// <returns>Json to be used to execute this query by Elasticsearch.</returns>
        JObject CreateBody()
        {
            var root = new JObject();

           

            var queryRoot = root;
            





            long? size = searchRequest.Size ?? 10000L;
            if (size.HasValue)
                root.Add("size", size.Value);

            //if (connection.Timeout != TimeSpan.Zero)
            //    root.Add("timeout", Format(connection.Timeout));

            return root;
        }

        public void Fill()
        {
            var root = new JObject();
            var queryRoot = root;

            if (searchRequest.Query != null)
            {
                long? size = searchRequest.Size ?? 10000L;
                if (size.HasValue)
                    queryRoot.Add("size", size.Value);
                queryRoot.Add("query", Build(QueryCriteriaRewriter.Compensate(searchRequest.Query)));
                
            }
            FullQuery = root;
        }



        static JArray Build(IEnumerable<SortOption> sortOptions)
        {
            return new JArray(sortOptions.Select(Build));
        }

        static object Build(SortOption sortOption)
        {
            if (String.IsNullOrEmpty(sortOption.UnmappedType))
                return sortOption.Ascending
                    ? (object)sortOption.Name
                    : new JObject(new JProperty(sortOption.Name, "desc"));

            var properties = new List<JProperty> { new JProperty("unmapped_type", sortOption.UnmappedType) };
            if (!sortOption.Ascending)
                properties.Add(new JProperty("order", "desc"));

            return new JObject(new JProperty(sortOption.Name, new JObject(properties)));
        }

        JObject Build(ICriteria criteria)
        {
            if (criteria == null)
                return null;

            if (criteria is RangeCriteria)
                return Build((RangeCriteria)criteria);

            if (criteria is RegexpCriteria)
                return Build((RegexpCriteria)criteria);

            if (criteria is PrefixCriteria)
                return Build((PrefixCriteria)criteria);

            if (criteria is TermCriteria)
                return Build((TermCriteria)criteria);

            if (criteria is TermsCriteria)
                return Build((TermsCriteria)criteria);

            if (criteria is NotCriteria)
                return Build((NotCriteria)criteria);

            if (criteria is QueryStringCriteria)
                return Build((QueryStringCriteria)criteria);

            if (criteria is MatchAllCriteria)
                return Build((MatchAllCriteria)criteria);

            if (criteria is BoolCriteria)
                return Build((BoolCriteria)criteria);
            if (criteria is MatchCriteria)
                return Build((MatchCriteria)criteria);
            if (criteria is CollectionContainsCriteria)
                return Build((CollectionContainsCriteria)criteria);
          
            // Base class formatters using name property

            if (criteria is SingleFieldCriteria)
                return Build((SingleFieldCriteria)criteria);

            if (criteria is CompoundCriteria)
                return Build((CompoundCriteria)criteria);

            throw new InvalidOperationException($"Unknown criteria type '{criteria.GetType()}'");
        }

        static JObject Build(Highlight highlight)
        {
            var fields = new JObject();

            foreach (var field in highlight.Fields)
                fields.Add(new JProperty(field, new JObject()));

            var queryStringCriteria = new JObject(new JProperty("fields", fields));

            if (!string.IsNullOrWhiteSpace(highlight.PostTag))
                queryStringCriteria.Add(new JProperty("post_tags", new JArray(highlight.PostTag)));
            if (!string.IsNullOrWhiteSpace(highlight.PreTag))
                queryStringCriteria.Add(new JProperty("pre_tags", new JArray(highlight.PreTag)));

            return queryStringCriteria;
        }

        static JObject Build(QueryStringCriteria criteria)
        {
            var unformattedValue = criteria.Value; // We do not reformat query_string

            JObject queryStringCriteria;
            //if (criteria.Nested)
            //{
            //    queryStringCriteria = new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", unformattedValue))));
            //}
            //else
            //{
                queryStringCriteria = new JObject(new JProperty("query", unformattedValue));
            
          

            if (criteria.Fields.Any())
                queryStringCriteria.Add(new JProperty("fields", new JArray(criteria.Fields)));


            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(new JProperty(criteria.Name, queryStringCriteria))))));
            }
            else
            {

                return new JObject(new JProperty(criteria.Name, queryStringCriteria));
            }
        }

        JObject Build(CollectionContainsCriteria criteria)
        {
            var innerQ = BuildLowerQuery(criteria);
            
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", innerQ))));
            }
            else
            {
              
                return new JObject(new JProperty("query", innerQ));
            }
        }
        JObject BuildLowerQuery(CollectionContainsCriteria criteria)
        {
            var query = new JObject();
            if (criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.Match)
            {
                return new JObject(new JProperty("match", new JObject(new JProperty(criteria.Field, new JObject(new JProperty("query", criteria.Value.ToString()))))));
            }
            if (criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.Term)
            {
                return  new JObject(new JProperty("term", new JObject(new JProperty(criteria.Field, new JObject(new JProperty("value", criteria.Value.ToString()))))));
            }
            if (criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.LT ||
                criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.LTE ||
                criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.GT ||
                criteria.ComparisonTypeValue == CollectionContainsCriteria.ComparisonType.GTE
             )
            {
                return  new JObject(new JProperty("range", new JObject(criteria.Field, new JObject(new JProperty(criteria.CompareValue, criteria.Value))))); 
            }
            return null;
        }


        JObject Build(RangeCriteria criteria)
        {

            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", new JObject(
                new JProperty(criteria.Name,
                    new JObject(new JProperty(criteria.Field,
                        new JObject(criteria.Specifications.Select(s =>
                            new JProperty(s.Name, mapping.FormatValue(criteria.Member, s.Value))).ToList())))))))));
            }
            else
            {
                // Range filters can be combined by field
                return new JObject(
                    new JProperty(criteria.Name,
                        new JObject(new JProperty(criteria.Field,
                            new JObject(criteria.Specifications.Select(s =>
                                new JProperty(s.Name, mapping.FormatValue(criteria.Member, s.Value))).ToList())))));
            }
        }

        static JObject Build(RegexpCriteria criteria)
        {

            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", 
                    new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, criteria.Regexp))))))));
            }
            else
            {
                return new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, criteria.Regexp))));
            }
        }

        static JObject Build(PrefixCriteria criteria)
        {
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, criteria.Prefix))))))));
            }
            else
            {
                return new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, criteria.Prefix))));
            }
        }

        JObject Build(TermCriteria criteria)
        {
            if (criteria.Nested)
            {
                //new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", 
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", new JObject(
                new JProperty(criteria.Name, new JObject(
                    new JProperty(criteria.Field, mapping.FormatValue(criteria.Member, criteria.Value)))))))));
            }
            else
            {


                return new JObject(
                    new JProperty(criteria.Name, new JObject(
                        new JProperty(criteria.Field, mapping.FormatValue(criteria.Member, criteria.Value)))));
            }
        }

        JObject Build(TermsCriteria criteria)
        {
            JObject termsCriteria;
            if (criteria.Nested)
            {
                termsCriteria = new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(
                new JProperty(criteria.Field,
                    new JArray(criteria.Values.Select(x => mapping.FormatValue(criteria.Member, x)).Cast<object>().ToArray())))))));
            }
            else
            {
                 termsCriteria = new JObject(
                    new JProperty(criteria.Field,
                        new JArray(criteria.Values.Select(x => mapping.FormatValue(criteria.Member, x)).Cast<object>().ToArray())));
            }

            if (criteria.ExecutionMode.HasValue)
                termsCriteria.Add(new JProperty("execution", criteria.ExecutionMode.GetValueOrDefault().ToString()));

            return new JObject(new JProperty(criteria.Name, termsCriteria));
        }

        static JObject Build(SingleFieldCriteria criteria)
        {
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(new JProperty(criteria.Name, new JObject(new JProperty("field", criteria.Field))))))));
            }
            else
            {
                return new JObject(new JProperty(criteria.Name, new JObject(new JProperty("field", criteria.Field))));
            }
        }

        JObject Build(NotCriteria criteria)
        {
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(new JProperty("bool", new JObject(new JProperty(criteria.Name, Build(criteria.Criteria)))))))));
            }
            else
            {
                return new JObject(new JProperty("bool", new JObject(new JProperty(criteria.Name, Build(criteria.Criteria)))));
            }
        }

        static JObject Build(MatchAllCriteria criteria)
        {
            return new JObject(new JProperty(criteria.Name));
        }

        JObject Build(MatchCriteria criteria)
        {
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query", new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, new JObject(new JProperty("query", mapping.FormatValue(criteria.Member, criteria.Value)))))))))));
            }
            else
            {
                return new JObject(new JProperty(criteria.Name, new JObject(new JProperty(criteria.Field, new JObject(new JProperty("query", mapping.FormatValue(criteria.Member, criteria.Value)))))));
            }
        }
        

        JObject Build(CompoundCriteria criteria)
        {
            // A compound criteria with one item can be collapsed
            return criteria.Criteria.Count == 1
                ? Build(criteria.Criteria.First())
                : new JObject(new JProperty(criteria.Name, new JArray(criteria.Criteria.Select(Build).ToList())));
        }

        JObject Build(BoolCriteria criteria)
        {
            if (criteria.Nested)
            {
                return new JObject(new JProperty("nested", new JObject(new JProperty("path", criteria.Path), new JProperty("query",
                    new JObject(new JProperty(criteria.Name, new JObject(BuildProperties(criteria))))))));
            }
            else
            {
                return new JObject(new JProperty(criteria.Name, new JObject(BuildProperties(criteria))));
            }
        }

        IEnumerable<JProperty> BuildProperties(BoolCriteria criteria)
        {
            if (criteria.Must.Any())
                yield return new JProperty("must", new JArray(criteria.Must.Select(Build)));

            if (criteria.MustNot.Any())
                yield return new JProperty("must_not", new JArray(criteria.MustNot.Select(Build)));

            if (criteria.Should.Any())
            {
                yield return new JProperty("should", new JArray(criteria.Should.Select(Build)));
                yield return new JProperty("minimum_should_match", 1);
            }
        }

        internal static string Format(TimeSpan timeSpan)
        {
            if (timeSpan.Milliseconds != 0)
                return timeSpan.TotalMilliseconds.ToString(transportCulture);

            if (timeSpan.Seconds != 0)
                return timeSpan.TotalSeconds.ToString(transportCulture) + "s";

            return timeSpan.TotalMinutes.ToString(transportCulture) + "m";
        }
    }
}
