using System;
using System.Collections.Generic;
using Nest;

namespace ElasticsearchQuery.Tests.Models
{
    public class NestedMockModel
    {
        public long Id { get; set; }

        [Keyword]
        public string Name { get; set; }

        public DateTime Date { get; set; }

        [Nested]
        public IList<MockModel> MockModels { get; set; }

        public double Price { get; set; }
    }
}
