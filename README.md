# ElasticSearchQuery
Its a simple IQueryable implementation for ElasticSearch built with Netstandard 2.0.

## Usage
 ```csharp
 
 var client = new ElasticClient();
 var query = ElasticSearchQueryFactory.CreateQuery<Product>(client);
 
 ```
 
See the Test project for the queries currently supported.
 
## Custom Index/Type Mapping
  ```csharp
 
 ElasticQueryMapper.Map(typeof(Product), indexName, indexType);
 
 ```
## Suported Nest Versions

6.0.0 - 6.6.0

## TODO

Add Custom Map for properties (Columns names / Types).

Add better support for fulltext queries.

Improve support for linq queries.

## Latest Work
Added Count support.

Added support for TermsQuery (collections contains methods)

Added Custom Map for class (Index/Type names).

Added Take and Skip support.

Added OrderBy support.

## Under development

We have some improvements and implementations to do.
Any help is welcome

## More About ElasticSearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
