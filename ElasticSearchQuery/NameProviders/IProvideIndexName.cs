using System;

namespace ElasticsearchQuery.NameProviders
{
    public interface IProvideIndexName
    {
        string GetIndexName(Type type);
    }
}
