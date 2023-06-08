using System;

namespace ElasticSearchQuery
{
    public class ElasticResponseException
    {
        public ElasticResponseException(dynamic elasticResponse, Type elementType) 
        {
            if (elasticResponse.OriginalException != null)
            {
                throw new Exception(elasticResponse.OriginalException.Message, elasticResponse.OriginalException);
            }
            else
            {
                throw new Exception(elasticResponse.ServerError != null ? elasticResponse.ServerError.Error.Reason : $"Elastic search failed for {elementType}.");
            }
        }
    }
}
