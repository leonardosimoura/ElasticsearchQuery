# ElasticSearchQuery
Its a simple IQueryable implementation for ElasticSearch

## Under development

Some queries are suported for now.(see the Test project)

Is an early development. Have a lot of improvements and implementations to do.

Using Netstandard 2.0


## Usage
 ```csharp
 
 var client = new ElasticClient();
 var query = ElasticSearchQueryFactory.CreateQuery<Product>(client);
 
 ```
## Custom Index/Type Mapping
  ```csharp
 
 ElasticQueryMapper.Map(typeof(Product), indexName, indexType);
 
 ```
## Suported Nest Versions

6.0.0 - 6.6.0

## TODO

Add Custom Map for properties (Columns names / Types).

Improve support for linq queries.

## Latest Work
Added support for TermsQuery (collections contains methods)

Added Custom Map for class (Index/Type names).

Added Take and Skip support.

Added OrderBy support.

## More About ElasticSearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
