using ElasticSearchQuery.Tests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticsearchQueryLib.Tests
{
    public class NestedMockModel
    {
        public long Id { get; set; }
        public string ProductName { get; set; }
        public IList<MockModel> MockModels { get; set; }
    }
}
