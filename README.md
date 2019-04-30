# ElasticSearchQuery
Its a simple IQueryable implementation for ElasticSearch

## under development

Some queries are suported for now.(see the Test project)

Is an early development. Have a lot of improvements and implementations to do.

Using Netstandard 2.0


##Usage
 ```csharp
 
 var client = new ElasticClient();
 var query = ElasticSearchQueryFactory.CreateQuery<Product>(client);
 
 ```

## Suported Nest Versions

6.0.0 - 6.6.0

## TODO

Add Custom Map for class (Index name / Type name / Columns names). Currently it use the name of class in camelcase


Improve support for linq queries.

## Latest Work
Add Take and Skip support (Added).

Add OrderBy support (Added).

## More About ElasticSearch
https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
