using System.ComponentModel;

namespace ElasticsearchQuery.Tests.Models
{
    public class MockModel
    {
        public MockModel()
        {
        }

        public MockModel(int id, string name)
        {
            Id = id;
            Name = name;
        }

        [DisplayName("mockModels.id")]
        public int Id { get; set; }

        [DisplayName("mockModels.name")]
        public string Name { get; set; }
    }
}
