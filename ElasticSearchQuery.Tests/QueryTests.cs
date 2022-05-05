using Bogus;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using ElasticLinq.Mapping;
using ElasticLinq.Logging;
using ElasticLinq;

namespace ElasticsearchQuery.Tests
{
    public class QueryTests 
    {
        private IElasticClient ObterCliente()
        {
            // docker run --name elasticsearch --restart=always -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -d docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.1
            var node = new Uri("http://localhost:9200/");
            var settings = new ConnectionSettings(node);
            settings.ThrowExceptions();
            settings.EnableDebugMode();
            settings.DefaultIndex("producttest");
            settings.DefaultMappingFor<ProductTest>(m => m.IdProperty(p => p.ProductId).IndexName("producttest"));
            settings.DefaultMappingFor<ProductTestMultiple>(m => m.IdProperty(p => p.ProductId).IndexName("producttestm"));
            var client = new ElasticClient(settings);
            return client;
        }

        private ElasticMapping GetMap()
        {
            var map = new ElasticMapping(false, false, false, false, EnumFormat.Integer);
            return map;
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

        private void CreateNewTest(IElasticClient client, string indexName = "producttestm", string indexType = "producttestm")
        {
            client.Indices.Create(indexName, cr =>
                       cr.Map<ProductTestMultiple>(m =>
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

        private void AddData(params ProductTestMultiple[] data)
        {
            var client = ObterCliente();

            if (client.Indices.Exists(Indices.Index("producttestm")).Exists)
                client.DeleteByQuery<ProductTestMultiple>(a => a.Query(q => q.MatchAll())
                    .Index("producttestm"));
            else
                CreateNewTest(client, "producttestm", "producttestm");

            client.IndexMany(data, "producttestm");

            client.Indices.Refresh("producttestm");
        }

        private IEnumerable<ProductTest> GenerateData(int size, params ProductTest[] additionalData)
        {
            var cat = new Faker<Category>("pt_BR")
                 .RuleFor(o => o.Id, f => f.Random.Int())
                 .RuleFor(o => o.CategoryId, f => f.Random.Int() % 2 == 0 ? 2 : 3)
                 .RuleFor(o => o.CategoryType, f => f.Random.Int() % 2 == 0 ? "BACON" : "OTHER")
                 .RuleFor(o => o.Name, f => f.Random.Int() % 2 == 0 ? "F" : "G");

            var testType = new Faker<TestType>("pt_BR")
                .RuleFor(o => o.MarketId, f => f.Random.Int(1, 10))
                .RuleFor(o => o.Id, f => f.Random.Int())
                .RuleFor(o => o.MemberName, f => (f.Random.Int() % 2 == 0 ? "BOB" : "STEVE"))
                .RuleFor(o => o.ProductType, f=> (f.Random.Int() % 2 == 0 ? "GOOD" : "BAD"))
                .RuleFor(o => o.VegetableId, f=> f.Random.Int(1,5))
                .RuleFor(o => o.Category, f => cat);

            


            var testsProducts = new Faker<ProductTest>("pt_BR")    
                                .RuleFor(o => o.Name ,f => f.Commerce.ProductName())
                                .RuleFor(o => o.Price, f => f.Finance.Amount(1))
                                .RuleFor(o => o.ProductId, f => Guid.NewGuid())
                                .RuleFor(o => o.Institution, f => (f.Random.Int() % 2 == 0 ? "SOMEWHERE" : "ELSE"))
                                .RuleFor(o => o.Test, f => testType)
                                
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

            var productsIds = products.Select(s => s.ProductId).ToList();

            query = query.Where(w => productsIds.Contains(w.ProductId));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.True(result.All(a => products.Any(p => p.ProductId == a.ProductId)));
        }


        [Fact]
        public void CheckContainsNested()
        {
            var cat1 = new Category(1, 2, "BACON", "F");
            var cat2 = new Category(2, 3, "OTHER", "G");


            var t = new TestType(1, 1, "JOHN");
            t.Category = cat1;
            var c = new TestType(1, 1, "BOB");
            c.Category = cat1;
            var se = new TestType(1, 2, "STEVE");
            se.Category = cat2;
            var be = new TestType(1, 3, "JAMES");

            var p = new List<TestType>() { t, c, se };
            var d = new List<TestType>() { t, c, se, be };

            var products = new ProductTest[]
            {
                new ProductTest(Guid.NewGuid(), "Product A", 99,t),
                new ProductTest(Guid.NewGuid(), "Product B", 150,t),
                new ProductTest(Guid.NewGuid(), "Product C", 200,t ),
                new ProductTest(Guid.NewGuid(), "Product D", 300, t),
                new ProductTest(Guid.NewGuid(), "Product E", 300,c),
                new ProductTest(Guid.NewGuid(), "Product F", 300, c),
                new ProductTest(Guid.NewGuid(), "Product G", 300, se),
                new ProductTest(Guid.NewGuid(), "Product H", 300, se),
                new ProductTest(Guid.NewGuid(), "Product I", 300, se)



            };



            var productList = GenerateData(1150, products);

            AddData(productList.ToArray());

            var client = ObterCliente();




            var q2 = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            q2 = q2.Where(a => a.Test.Category.CategoryType.Contains("BAC") || a.Test.MarketId == 2);


            var q2res = q2.ToList();
            Assert.NotEmpty(q2res);

            var ac = productList.Count(a => a.Test.Category.CategoryType.Contains("BAC") || a.Test.MarketId == 2);
            var bc = productList.Where(a => a.Test.Category.CategoryType.Contains("BAC") || a.Test.MarketId == 2);
            Assert.True(ac == q2res.Count);
            Assert.True(q2res.All(a => a.Test.Category.CategoryType.Contains("BAC") || a.Test.MarketId == 2));
        }
        [Fact]
        public void NestedCount()
        {
            var cat1 = new Category(1, 2, "BACON", "F");
            var cat2 = new Category(2, 3, "OTHER", "G");


            var t = new TestType(1, 1, "JOHN");
            t.Category = cat1;
            var c = new TestType(1, 1, "BOB");
            c.Category = cat1;
            var se = new TestType(1, 2, "STEVE");
            se.Category = cat2;
            var be = new TestType(1, 3, "JAMES");

            var p = new List<TestType>() { t, c, se };
            var d = new List<TestType>() { t, c, se, be };

            var products = new ProductTest[]
            {
                new ProductTest(Guid.NewGuid(), "Product A", 99,t),
                new ProductTest(Guid.NewGuid(), "Product B", 150,t),
                new ProductTest(Guid.NewGuid(), "Product C", 200,t ),
                new ProductTest(Guid.NewGuid(), "Product D", 300, t),
                new ProductTest(Guid.NewGuid(), "Product E", 300,c),
                new ProductTest(Guid.NewGuid(), "Product F", 300, c),
                new ProductTest(Guid.NewGuid(), "Product G", 300, se),
                new ProductTest(Guid.NewGuid(), "Product H", 300, se),
                new ProductTest(Guid.NewGuid(), "Product I", 300, se)



            };



            var productList = GenerateData(1150, products);

            AddData(productList.ToArray());

            var client = ObterCliente();


            var q2 = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            var cCount = q2.Count(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2);


            

            var ac = productList.Count(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2);
            
            Assert.True(ac == cCount);
           
        }

   

        [Fact]
        public void NestedList()
        {
            var cat1 = new Category(1, 2, "BACON", "F");
            var cat2 = new Category(2, 3, "OTHER", "G");


            var t = new TestType(1, 1, "JOHN", 1);
            t.Category = cat1;
            var c = new TestType(1, 1, "BOB");
            c.Category = cat1;
            var se = new TestType(1, 2, "STEVE");
            se.Category = cat2;
            var be = new TestType(1, 3, "JAMES", 1);
            be.Category = cat1;

            var p = new List<TestType>() { t, c, se };
            var d = new List<TestType>() { t, c, se, be };

            var products = new ProductTestMultiple[]
            {
                new ProductTestMultiple(Guid.NewGuid(), "Product A", 99,p),
                new ProductTestMultiple(Guid.NewGuid(), "Product B", 150,p),
                new ProductTestMultiple(Guid.NewGuid(), "Product C", 200,d ),
                new ProductTestMultiple(Guid.NewGuid(), "Product D", 300, p),
                new ProductTestMultiple(Guid.NewGuid(), "Product E", 300,d),
                new ProductTestMultiple(Guid.NewGuid(), "Product F", 300, d),
                new ProductTestMultiple(Guid.NewGuid(), "Product G", 300, d),
                new ProductTestMultiple(Guid.NewGuid(), "Product H", 300, d),
                new ProductTestMultiple(Guid.NewGuid(), "Product I", 300, d)



            };

            var client = ObterCliente();

            AddData(products.ToArray());

            var q0 = ElasticSearchQueryFactory.CreateQuery<ProductTestMultiple>(client, GetMap(), NullLog.Instance);


            //q0 = q0.Where(a => a.Price == 200 && a.Name == "JOE");

            q0 = q0.Where(a => a.Tests.Any(e => e.MemberName == "JAMES"));
            var q0res = q0.ToList();

            Assert.NotEmpty(q0res);

            var q0ac = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES"));


            Assert.True(q0ac == q0res.Count);
            Assert.True(q0res.All(a => a.Tests.Any(e => e.MemberName == "JAMES")));


            var q2 = ElasticSearchQueryFactory.CreateQuery<ProductTestMultiple>(client, GetMap(), NullLog.Instance);


            //q2 = q2.Where(a => a.Price == 200 && a.Name == "JOE");

            q2 = q2.Where(a => a.Tests.Any(e => e.MemberName == "JAMES" || e.MarketId == 2));
            var q2res = q2.ToList();

            Assert.NotEmpty(q2res);

            var ac = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES" || e.MarketId == 2));


            Assert.True(ac == q2res.Count);
            Assert.True(q2res.All(a => a.Tests.Any(e => e.MemberName == "JAMES" || e.MarketId == 2)));


            /////////////////////

            var q1 = ElasticSearchQueryFactory.CreateQuery<ProductTestMultiple>(client, GetMap(), NullLog.Instance);



            q1 = q1.Where(a => a.Tests.Any(e => e.MemberName == "JAMES") && a.Price <= 200);
            var q1res = q1.ToList();

            Assert.NotEmpty(q1res);

            var fullList1 = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES") && a.Price <= 200);

            Assert.True(fullList1 == q1res.Count);
            Assert.True(q1res.All(a => a.Tests.Any(e => e.MemberName == "JAMES") && a.Price <= 200));
            //

            var q3 = ElasticSearchQueryFactory.CreateQuery<ProductTestMultiple>(client, GetMap(), NullLog.Instance);



            q3 = q3.Where(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1));
            var q3res = q3.ToList();

            Assert.NotEmpty(q3res);

            var fullList = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1));
            var ef = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1));

            Assert.True(fullList == q3res.Count);
            Assert.True(q3res.All(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1)));

            ///////////////
            ///
            var q4 = ElasticSearchQueryFactory.CreateQuery<ProductTestMultiple>(client, GetMap(), NullLog.Instance);



            q4 = q4.Where(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1) && a.Name == "Product D");
            
            var q4res = q4.ToList();

            Assert.NotEmpty(q4res);

            var fullList2 = products.Count(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1) && a.Name == "Product D");
            

            Assert.True(fullList2 == q4res.Count);
            Assert.True(q4res.All(a => a.Tests.Any(e => e.MemberName == "JAMES" && e.MarketId == 2 || e.VegetableId == 1) && a.Name == "Product D"));
        }

        [Fact]
        public void CheckNested()
        {
            var cat1 = new Category(1, 2, "BACON", "F");
            var cat2 = new Category(2, 3, "OTHER", "G");

            
            var t = new TestType(1, 1, "JOHN");
            t.Category = cat1;
            var c = new TestType(1, 1, "BOB");
            c.Category = cat1;
            var se = new TestType(1, 2, "STEVE");
            se.Category = cat2;
            var be = new TestType(1, 3, "JAMES");
            be.Category = cat1;

            var p = new List<TestType>() { t, c, se };
            var d = new List<TestType>() { t, c, se, be };
            
            var products = new ProductTest[]
            {
                  new ProductTest(Guid.NewGuid(), "Product A", 99,t),
                new ProductTest(Guid.NewGuid(), "Product B", 150,t),
                new ProductTest(Guid.NewGuid(), "Product C", 200,t ),
                new ProductTest(Guid.NewGuid(), "Product D", 300, t),
                new ProductTest(Guid.NewGuid(), "Product E", 300,c),
                new ProductTest(Guid.NewGuid(), "Product F", 300, c),
                new ProductTest(Guid.NewGuid(), "Product G", 300, se),
                new ProductTest(Guid.NewGuid(), "Product H", 300, se),
                new ProductTest(Guid.NewGuid(), "Product I", 300, se)



            };

            

            var productList = GenerateData(1150, products);
           
            AddData(productList.ToArray());
            
            var client = ObterCliente();
          
            
         

            var q2 = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            q2 = q2.Where(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2);


            var q2res = q2.ToList();
            Assert.NotEmpty(q2res);
           
            var ac = productList.Count(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2);
            var bc = productList.Where(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2);
            Assert.True(ac == q2res.Count);
            Assert.True(q2res.All(a => a.Test.MemberName == "JOHN" || a.Test.MarketId == 2));

            //-------------------------------------------------------------------------------


            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
           query = query.Where(a => a.Test.Category.CategoryId == 2 || a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON" || a.Test.MemberName == "JOHN");
            q2res = query.ToList();
            Assert.NotEmpty(q2res);

            var ace = productList.Count(a => a.Test.Category.CategoryId == 2 || a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON" || a.Test.MemberName == "JOHN");
            var bce = productList.Where(a => a.Test.Category.CategoryId == 2 || a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON" || a.Test.MemberName == "JOHN");
            Assert.True(ace == q2res.Count);
            Assert.True(q2res.All(a => a.Test.Category.CategoryId == 2 || a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON" || a.Test.MemberName == "JOHN"));

            //-------------------------------------------------------------------------------

            var query2 = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query2 = query2.Where(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON") || a.Test.MemberName == "JOHN");
            q2res = query2.ToList();
            Assert.NotEmpty(q2res);

            var acer = productList.Count(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON") || a.Test.MemberName == "JOHN");
            var bcer = productList.Where(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON") || a.Test.MemberName == "JOHN");
            Assert.True(acer == q2res.Count);
            Assert.True(q2res.All(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType == "BACON") || a.Test.MemberName == "JOHN"));

            //-------------------------------------------------------------------------------

            var query3 = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query3 = query3.Where(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType.Contains("BAC")) || a.Test.MemberName == "JOHN");
            q2res = query3.ToList();
            Assert.NotEmpty(q2res);

            var acere = productList.Count(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType.Contains("BAC")) || a.Test.MemberName == "JOHN");
            var bcere = productList.Where(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType.Contains("BAC")) || a.Test.MemberName == "JOHN");
            Assert.True(acere == q2res.Count);
            Assert.True(q2res.All(a => a.Test.Category.CategoryId == 2 && (a.Test.MarketId == 2 || a.Test.Category.CategoryType.Contains("BAC")) || a.Test.MemberName == "JOHN"));

            //-------------------------------------------------------------------------------

        }

        [Fact]
        public void OrderByAndWhere()
        {
            var product = new ProductTest(Guid.NewGuid(), "Product A", 99);

            var productList = GenerateData(1000, product);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

            query = query.Where(w => w.Price == 99).OrderBy(o => o.Name).ThenBy(o => o.Price);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f=>  f.ProductId == product.ProductId);           
            Assert.Equal(99, result.First().Price);
        }

     

        [Fact]
        public void OrderBy()
        {
            var productList = GenerateData(1000);

            AddData(productList.ToArray());
            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

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


            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

            query = query.Take(take).Skip(skip);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Equal(take, result.Count);
        }

        [Fact]
        public void EndsWithDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            var productList = GenerateData(1000, produto);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !w.Name.EndsWith("Test"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.Name.EndsWith("zzzzzzz"));

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void StartsWithDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "TestProduct", 9.9M);

            var productList = GenerateData(1000, produto);

            AddData(productList.ToArray());

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !w.Name.StartsWith("Test"));

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.Name.StartsWith("zzzzzzzzzzz"));

            var result = query.ToList();
            Assert.Empty(result);
        }


        [Fact]
        public void ContainsDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !w.Name.Contains("Test"));

            var result = query.ToList();
            Assert.Empty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
        }

        [Fact]
        public void ContainsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.ProductId != Guid.Empty);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

       


        [Fact]
        public void NotEqualsDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !(w.ProductId != Guid.Empty));

            var result = query.ToList();
            Assert.Empty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
        }


        [Fact]
        public void NotEqualsInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.ProductId != pId);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact]
        public void EqualsDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !(w.ProductId == pId));

            var result = query.ToList();
            Assert.Empty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
        }

        [Fact]
        public void EqualsValid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;
            var pNome = produto.Name;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.Price < 50);

            var result = query.ToList();
            Assert.NotEmpty(result);
            Assert.Contains(result, f => f.ProductId == pId);
        }

        [Fact]
        public void LessThanDeny()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var pId = produto.ProductId;

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => !(w.Price < 50));

            var result = query.ToList();
            Assert.Empty(result);
            Assert.DoesNotContain(result, f => f.ProductId == pId);
        }


        [Fact]
        public void LessThanInvalid()
        {
            var produto = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);

            AddData(produto);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            query = query.Where(w => w.Price < 1);

            var result = query.ToList();
            Assert.Empty(result);
        }

        [Fact(Skip = "Not Supported")]
        public void AggregationSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
            var count = query.Where(w => w.Price < 10M).Count();

            Assert.Equal(4, count);
        }

        [Fact(Skip = "Not supported")]
        public void LinqSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);
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

        [Fact(Skip = "Not supported")]
        public void LinqNewGroupSum()
        {
            var produto1 = new ProductTest(Guid.NewGuid(), "ProductTest", 9.9M);
            var produto2 = new ProductTest(Guid.NewGuid(), "ProductTest 2", 5M);

            AddData(produto1, produto2);

            var client = ObterCliente();

            var query = ElasticSearchQueryFactory.CreateQuery<ProductTest>(client, GetMap(), NullLog.Instance);

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

    [ElasticsearchType(Name = "producttestm")]
    public class ProductTestMultiple
    {
        public ProductTestMultiple()
        {

        }
        //public ProductTest(Guid productId, string name, decimal price,TestType t = null)
        //{
        public ProductTestMultiple(Guid productId, string name, decimal price, IList<TestType> t = null)
        {
            ProductId = productId;
            Name = name;
            NameAsText = name;
            Price = price;
            Tests = t;
            //Test = t;

        }

        public Guid ProductId { get; set; }
        public string Name { get; set; }
        public string NameAsText { get; set; }
        public string Institution { get; set; }
        public decimal Price { get; set; }

        [Nested]
        public IList<TestType> Tests { get; set; }

        //[Nested]
        //public TestType Test { get; set; }
    }


    [ElasticsearchType(Name= "producttest")]
    public class ProductTest
    {
        public ProductTest()
        {

        }
        public ProductTest(Guid productId, string name, decimal price, TestType t = null)
        {

            ProductId = productId;
            Name = name;
            NameAsText = name;
            Price = price;

            Test = t;

        }

        public Guid ProductId { get; set; }
        public string Name { get; set; }
        public string NameAsText { get; set; }
        public string Institution { get; set; }
        public decimal Price { get; set; }



        [Nested]
        public TestType Test { get; set; }
    }

    public class TestType
    {
        public TestType()
        {

        }
        public TestType(int id, int marketId, string memberName, int vegetableId)
        {
            Id = id;
            MarketId = marketId;
            MemberName = memberName;
            VegetableId = vegetableId;
        }
        public TestType(int id, int marketId, string memberName)
        {
            Id = id;
            MarketId = marketId;
            MemberName = memberName;
            
        }
        public int Id { get; set; }
        public int MarketId { get; set; }
        public string MemberName { get; set; }
        public string ProductType { get; set; }
        public int VegetableId { get; set; }
        public Category Category { get; set; }
    }
    public class Category
    {
        public Category() { }
        public Category(int id, int cid, string type, string name)
        {
            Id = id;
            CategoryId = cid;
            CategoryType = type;
            Name = name;
        }
        public int CategoryId { get; set; }
        public int Id { get; set; }
        public string CategoryType { get; set; }
        public string Name { get; set; }

    }
}
