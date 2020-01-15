using Bogus;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ElasticsearchQuery.Extensions;
using Xunit;

namespace ElasticsearchQuery.Tests
{
    public class QueryTests
    {
        private IElasticClient ObterCliente()
        {
            var node = new Uri("http://192.168.99.104:9200/");
            var settings = new ConnectionSettings(node);
            settings.ThrowExceptions();
            settings.EnableDebugMode();
            settings.DefaultMappingFor<ProductTest>(m => m.IdProperty(p => p.ProductId));
            var client = new ElasticClient(settings);
            return client;
        }

        private void CreateIndexTest(IElasticClient client, string indexName = "producttest" , string indexType = "producttest")
        {
            client.Indices.Create(indexName, cr => 
                       cr.Map<ProductTest>( m =>
                              m.AutoMap()                              
                              .Properties(ps => ps
                                  .Text(p => p.Name(na => na.ProductId).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.Name).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.NameAsText).Analyzer("standard").Fielddata(true))
                                  .Number(p => p.Name(na => na.Price).Type(NumberType.Double))
                                  )));
        }

        private void AddData(params ProductTest[] data)
        {
            var client = ObterCliente();

            if (client.Indices.Exists(Indices.Index("producttest")).Exists)            
                client.DeleteByQuery<ProductTest>(a => a.Query(q => q.MatchAll())
                    .Index("producttest"));            
            else            
                CreateIndexTest(client);                           
            
            client.IndexMany(data, "producttest");

            client.Indices.Refresh("producttest");
        }

        private IEnumerable<ProductTest> GenerateData(int size, params ProductTest[] additionalData)
        {
            var testsProducts = new Faker<ProductTest>("pt_BR")    
                                .RuleFor(o => o.Name ,f => f.Commerce.ProductName())
                                .RuleFor(o => o.Price, f => f.Finance.Amount(1))
                                .RuleFor(o => o.ProductId, f => Guid.NewGuid())
                                .FinishWith((f, p) =>
                                {
                                    p.NameAsText = p.Name;
                                });

            return testsProducts.Generate(size).Union(additionalData);
        }

        [Theory]
        [InlineData("customindex", "customindex")]
        [InlineData("mycustomidx", "mycustomidx")]
        [InlineData("customindex", "mycustomidx")]
        [InlineData("productindex", "productindextype")]
        public void UsingCustomMapper(string indexName , string indexType)
        {
            var productList = GenerateData(1000);

            var client = ObterCliente();

            if (client.Indices.Exists(indexName).Exists)
                client.Indices.Delete(indexName);

            CreateIndexTest(client, indexName, indexType);

            client.IndexMany(productList, indexName);
            client.Indices.Refresh(indexName);

            ElasticQueryMapper.Map(typeof(ProductTest), indexName, indexType);

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var result = query.ToList();
            Assert.NotEmpty(result);
            ElasticQueryMapper.Clean();
        }

        [Fact]
        public void WhereWithCollectionContainsMethod()
        {

            var products = new ProductTest[]
            {
                new ProductTest(Guid.NewGuid(), "Product A", 99),
                new ProductTest(Guid.NewGuid(), "Product B", 150),
                new ProductTest(Guid.NewGuid(), "Product C", 200),
                new ProductTest(Guid.NewGuid(), "Product D", 300)
            };

            var productList = GenerateData(1000, products);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            var productsIds = products.Select(s => s.ProductId).ToList();

            query = query.Where(w => productsIds.Contains(w.ProductId));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.True(result.All(a => products.Any(p => p.ProductId == a.ProductId)));
        }

        [Fact]
        public void OrderByAndWhere()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product A", 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.Price == 99).OrderBy(o => o.Name).ThenBy(o => o.Price);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f=>  f.ProductId == product.ProductId);           
            Assert.Equal(99, result.First().Price);
        }

        [Fact]
        public void MatchPhrase()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product of category", 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.NameAsText.MatchPhrase("of category"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == product.ProductId);
            Assert.Equal(99, result.First().Price);
        }

        [Fact]
        public void MultiMatch()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product of category", 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.MultiMatch("category",t => t.NameAsText,t => t.Name));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == product.ProductId);
            Assert.Equal(99, result.First().Price);
        }

        [Fact]
        public void MultiMatchDeny()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product of category", 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => !w.MultiMatch("category", t => t.NameAsText, t => t.Name));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.DoesNotContain(result, f => f.ProductId == product.ProductId);
        }

        [Fact]
        public void Exists()
        {
            var product = new ProductTest(Guid.NewGuid(), null, 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.Exists(t => t.NameAsText));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.DoesNotContain(result, f => f.ProductId == product.ProductId);
        }

        [Fact]
        public void OrderBy()
        {
            var productList = GenerateData(1000);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.OrderBy(o => o.Name).ThenBy(o => o.Price);

            var result = query.ToList();

            var product = productList.OrderBy(o => o.Name).ThenBy(o => o.Price).First();

            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == product.ProductId);
            Assert.Equal(product.Price, result.First().Price);
        }

        [Fact]
        public void OrderByDescendingAndWhere()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product A", 150);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();


            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.Price == 150).OrderByDescending(o => o.Name).ThenByDescending(o => o.Price);

            var result = query.ToList();
            Assert.Contains(result, f => f.ProductId == product.ProductId);
            Assert.Equal(product.Price, result.First().Price);
        }

        [Fact]
        public void OrderByDescending()
        {
            var productList = GenerateData(1000);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.OrderByDescending(o => o.Name).ThenByDescending(o => o.Price);
            var product = productList.OrderByDescending(o => o.Name).ThenByDescending(o => o.Price).First();

            var result = query.ToList();            

            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == product.ProductId);
            Assert.Equal(product.Price, result.First().Price);
        }

        [Theory]
        [InlineData(0,10)]
        [InlineData(5, 50)]
        [InlineData(50, 30)]
        public void TakeSkipValid(int skip , int take)
        {
            var productList = GenerateData(1000);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Take(take).Skip(skip);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Equal(take, result.Count);
        }

        [Fact]
        public void EndsWithValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            var productList = GenerateData(1000, produto);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.EndsWith("st"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void EndsWithInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.EndsWith("zzzzzzz"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void StartsWithValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            var productList = GenerateData(1000, produto);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.StartsWith("Prod"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void StartsWithInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            var productList = GenerateData(1000, produto);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.StartsWith("zzzzzzzzzzz"));

            var result = query.ToList();
            Assert.Empty(result);
        }


        [Fact]
        public void ContainsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.Contains("Test"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void ContainsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.Contains("Coca-Cola"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void NotEqualsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId != Guid.Empty);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }


        [Fact]
        public void NotEqualsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId != pId);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void EqualsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId == pId);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void EqualsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId == Guid.Empty);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void GreaterThanInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Price > 50);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void GreaterThanValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Price > 1);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void LessThanValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Price < 50);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }


        [Fact]
        public void LessThanInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Price < 1);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void AggregationSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var totalPrice = query.Sum(s => s.Price);

            Assert.Equal(produto1.Price + produto2.Price, totalPrice);
        }

        [Fact]
        public void AggregationCount()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var count = query.Count();

            Assert.Equal(2, count);
        }

        [Fact]
        public void AggregationCountWithCoditions()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 25.5M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);
            var produto3 = new ProductTest(Guid.NewGuid(), "ProductTest 3", 6M);
            var produto4 = new ProductTest(Guid.NewGuid(), "ProductTest 4", 7M);
            var produto5 = new ProductTest(Guid.NewGuid(), "ProductTest 5", 8M);

            AddData(produto1, produto2, produto3, produto4, produto5);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var count = query.Where(w => w.Price < 10M).Count();

            Assert.Equal(4, count);
        }

        [Fact]
        public void LinqSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var result = (from o in query
                          group o by o.Name into g
                          select new
                          {
                              Produto = g.Key,
                              Total = g.Sum(o => o.Price),
                              Min = g.Min(o => o.Price),
                              Avg = g.Average(o => o.Price)
                          }).ToList();

            Assert.Equal(produto1.Price + produto2.Price, result.Sum(s => s.Total));
        }

        [Fact]
        public void LinqNewGroupSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            var result = (from o in query
                          group o by new { o.Name, o.ProductId } into g
                          select new
                          {
                              Produto = g.Key.Name,
                              Total = g.Sum(o => o.Price),
                              Min = g.Min(o => o.Price),
                              Avg = g.Average(o => o.Price)
                          }).ToList();

            Assert.Equal(produto1.Price + produto2.Price, result.Sum(s => s.Total));
        }
    }

    [ElasticsearchType(Name= "producttest")]
    public class ProductTest
    {
        public ProductTest()
        {

        }
        public ProductTest(Guid productId, string name, decimal price)
        {
            ProductId = productId;
            Name = name;
            NameAsText = name;
            Price = price;
        }

        public Guid ProductId { get; set; }
        public string Name { get; set; }
        public string NameAsText { get; set; }
        public decimal Price { get; set; }
    }
}
