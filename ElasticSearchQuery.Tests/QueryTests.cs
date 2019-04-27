using Nest;
using System;
using System.Linq;
using Xunit;

namespace ElasticSearchQuery.Tests
{
    public class QueryTests
    {
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

        private IElasticClient ObterCliente()
        {
            var node = new Uri("http://localhost:9200");
            var settings = new ConnectionSettings(node);
            settings.ThrowExceptions();
            settings.EnableDebugMode();
            settings.DefaultMappingFor<ProductTest>(m => m.IdProperty(p => p.ProductId));
            var client = new ElasticClient(settings);
            return client;
        }

        private void CreateIndexTest(IElasticClient client)
        {
            client.CreateIndex("ProductTest".ToLower(), cr => cr.Mappings(mp =>
                       mp.Map<ProductTest>(m =>
                              m.AutoMap()
                              .Properties(ps => ps
                                  .Text(p => p.Name(na => na.ProductId).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.Name).Analyzer("keyword").Fielddata(true))
                                  .Text(p => p.Name(na => na.NameAsText).Analyzer("standard").Fielddata(true))
                                  .Number(p => p.Name(na => na.Price).Type(NumberType.Double))
                                  ))));
        }


        [Fact]
        public void EndsWithValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.EndsWith("ste"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void EndsWithInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.EndsWith("aabbcc"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void StartsWithValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.StartsWith("Prod"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void StartsWithInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.StartsWith("Leite"));

            var result = query.ToList();
            Assert.Empty(result);
        }


        [Fact]
        public void ContainsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.Contains("Teste"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void ContainsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Name.Contains("Coca-Cola"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void NotEqualsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId != Guid.Empty);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }


        [Fact]
        public void NotEqualsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId != pId);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void EqualsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId == pId);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void EqualsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());


            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.ProductId == Guid.Empty);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void GreaterThanInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Price > 50);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void GreaterThanValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var pId = produto.ProductId;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Price > 1);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void LessThanValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var pId = produto.ProductId;

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Price < 50);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }


        [Fact]
        public void LessThanInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            query = query.Where(w => w.Price < 1);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void AggregacaoSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "Produto de Teste 2", 5M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto1, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Index(produto2, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
            var precoTotal = query.Sum(s => s.Price);

            Assert.Equal(produto1.Price + produto2.Price, precoTotal);
        }

        [Fact]
        public void LinqSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "Produto de Teste 2", 5M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto1, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Index(produto2, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);
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
            var produto1 = new ProductTest(Guid.NewGuid(), "Produto de Teste", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "Produto de Teste 2", 5M);
            var client = ObterCliente();

            if (client.IndexExists("ProductTest".ToLower()).Exists)
                client.DeleteIndex("ProductTest".ToLower());

            CreateIndexTest(client);

            client.Index(produto1, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Index(produto2, f => f.Index("ProductTest".ToLower()).Type("ProductTest".ToLower()));
            client.Refresh("ProductTest".ToLower());

            var query = ElasticSearchQueryFactory.GetQuery<ProductTest>(client);

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
}
