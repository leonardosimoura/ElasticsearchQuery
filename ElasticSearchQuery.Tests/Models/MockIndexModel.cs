using Nest;
using System.ComponentModel;

namespace ElasticsearchQuery.Tests.Models
{
    public class MockIndexModel
    {
        public MockIndexModel()
        {
        }

        public MockIndexModel(int id, string name)
        {
            Id = id;
            Name = name;
        }

        [DisplayName("mockModels.id")]
        public int Id { get; set; }

        [Keyword]
        [DisplayName("mockModels.name")]
        public string Name { get; set; }
    }
}
