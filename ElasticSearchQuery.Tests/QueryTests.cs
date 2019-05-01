using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace ElasticSearchQuery.Tests
{
    public class QueryTests
    {
        private IElasticClient ObterCliente()
        {
            var node = new Uri("http://localhost:9200/");
            var settings = new ConnectionSettings(node);
            settings.ThrowExceptions();
            settings.EnableDebugMode();
            settings.DefaultMappingFor<ProductTest>(m => m.IdProperty(p => p.ProductId));
            var client = new ElasticClient(settings);
            return client;
        }

        private void CreateIndexTest(IElasticClient client, string indexName = "producttest" , string indexType = "producttest")
        {
            client.CreateIndex(indexName, cr => cr.Mappings(mp =>
                       mp.Map<ProductTest>(indexType, m =>
                              m.AutoMap()                              
                              .Properties(ps => ps
                                  .Text(p => p.Name(na => na.ProductId).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.Name).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.NameAsText).Analyzer("standard").Fielddata(true))
                                  .Number(p => p.Name(na => na.Price).Type(NumberType.Double))
                                  ))));
        }

        private void AddData(params ProductTest[] data)
        {
            var client = ObterCliente();

            if (client.IndexExists("producttest").Exists)            
                client.DeleteByQuery<ProductTest>(a => a.Query(q => q.MatchAll())
                    .Index("producttest").Type("producttest"));            
            else            
                CreateIndexTest(client);                           
            
            client.IndexMany(data, "producttest", "producttest");

            client.Refresh("producttest");
        }

        [Theory]
        [InlineData("customindex", "customindex")]
        [InlineData("mycustomidx", "mycustomidx")]
        [InlineData("customindex", "mycustomidx")]
        [InlineData("productindex", "productindextype")]
        public void UsingCustomMapper(string indexName , string indexType)
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

            var client = ObterCliente();

            if (client.IndexExists(indexName).Exists)
                client.DeleteIndex(indexName);

            CreateIndexTest(client, indexName, indexType);

            client.IndexMany(productList, indexName, indexType);
            client.Refresh(indexName);

            ElasticQueryMapper.Map(typeof(ProductTest), indexName, indexType);

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var result = query.ToList();
            Assert.NotEmpty(result);
            ElasticQueryMapper.Clean();
        }

        [Fact]
        public void OrderByAndWhere()
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.Price == 99).OrderBy(o => o.Name).ThenBy(o => o.Price);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Single(result);
            Assert.Equal("ProductTest 99", result.First().Name);
            Assert.Equal(99, result.First().Price);
        }

        [Fact]
        public void OrderBy()
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.OrderBy(o => o.Name).ThenBy(o => o.Price);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Equal("ProductTest 0", result.First().Name);
            Assert.Equal(0, result.First().Price);
        }

        [Fact]
        public void OrderByDescendingAndWhere()
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

            AddData(productList.ToArray());
            var client = ObterCliente();


            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.Where(w => w.Price == 150).OrderByDescending(o => o.Name).ThenByDescending(o => o.Price);

            var result = query.ToList();
            Assert.Single(result);
            Assert.Equal("ProductTest 150", result.First().Name);
            Assert.Equal(150, result.First().Price);
        }

        [Fact]
        public void OrderByDescending()
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

            AddData(productList.ToArray());

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);

            query = query.OrderByDescending(o => o.Name).ThenByDescending(o => o.Price);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Equal("ProductTest 999", result.First().Name);
            Assert.Equal(999, result.First().Price);
        }

        [Theory]
        [InlineData(0,10)]
        [InlineData(5, 50)]
        [InlineData(50, 30)]
        public void TakeSkipValid(int skip , int take)
        {
            var productList = new List<ProductTest>();

            for (int i = 0; i < 1000; i++)
            {
                productList.Add(new ProductTest(Guid.NewGuid(), "ProductTest " + i, i));
            }

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

            AddData(produto);

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
            query = query.Where(w => w.Name.EndsWith("aabbcc"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void StartsWithValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

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

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            query = query.Where(w => w.Name.StartsWith("Leite"));

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
        public void AggregacaoSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client);
            var precoTotal = query.Sum(s => s.Price);

            Assert.Equal(produto1.Price + produto2.Price, precoTotal);
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
