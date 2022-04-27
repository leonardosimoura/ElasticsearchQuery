using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticLinq.Request.Criteria
{
    public abstract class CriteriaWrapper<T> 
        where T : ICriteria
    {

        public CriteriaWrapper(string pathName = null, bool isNested = false)
        {
            Path = pathName;
            Nested = isNested;
        }

        public string Path { get; set; }

        public bool Nested { get; set; }
    }
}

