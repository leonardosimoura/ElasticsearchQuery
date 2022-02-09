using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchQuery.Tests
{
    public class MockModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public MockModel()
        {

        }
        public MockModel(int id, string name)
        {
            Id = id;
            Name = name;
        }
    }
    public static class MockData
    {
        static public void AddData(List<MockModel> model)
        {
            model.Add(new MockModel(1, "obj1"));
            model.Add(new MockModel(2, "obj2"));
            model.Add(new MockModel(3, "obj3"));
            model.Add(new MockModel(4, "obj4"));
            model.Add(new MockModel(5, "obj5"));
            model.Add(new MockModel(6, "obj6"));
        }
    }
}