using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sitecore.ContentSearch.LuceneProvider;
using System.Text.RegularExpressions;
using System.Reflection;
using Sitecore.ContentSearch;
using Sitecore.Diagnostics;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    public class LuceneFieldNameTranslator : Sitecore.ContentSearch.LuceneProvider.LuceneFieldNameTranslator
    {
        private ILuceneProviderIndex index;
        public LuceneFieldNameTranslator(ILuceneProviderIndex index) : base(index)
        {
            Assert.ArgumentNotNull(index, "index");

            this.index = index;
        }

        public override IEnumerable<string> GetTypeFieldNames(string fieldName)
        {
            yield return fieldName;
            if (!fieldName.StartsWith("_"))
            {
                //yield return Regex.Replace(fieldName, @"(?<!\.)_", " ").Trim();
                StringBuilder MyStringBuilder = new StringBuilder(fieldName);
                MyStringBuilder.Replace("._", "$&#");
                MyStringBuilder.Replace('_', ' ');
                yield return MyStringBuilder.Replace("$&#", "._").ToString();
            }
        }
        // Sitecore.ContentSearch.LuceneProvider.LuceneFieldNameTranslator
        private Dictionary<string, List<string>> MapDocumentFieldsToTypeProperties(Type type, IEnumerable<string> documentFieldNames)
        {
            Dictionary<string, List<string>> map = documentFieldNames.ToDictionary((string f) => f, (string f) => this.GetTypeFieldNames(f).ToList<string>());
            Dictionary<string, List<string>> result = new Dictionary<string, List<string>>();
            base.ProcessProperties(type, map, ref result, "", "");
            return result;
        }
        // Sitecore.ContentSearch.LuceneProvider.LuceneFieldNameTranslator
        private Dictionary<string, List<string>> MapDocumentFieldsToTypeIndexer(Type type, IEnumerable<string> documentFieldNames)
        {
            Dictionary<string, List<string>> dictionary = documentFieldNames.ToDictionary((string f) => f, (string f) => this.GetTypeFieldNames(f).ToList<string>());
            IEnumerable<PropertyInfo> properties = base.GetProperties(type);
            foreach (PropertyInfo current in properties)
            {
                IIndexFieldNameFormatterAttribute indexFieldNameFormatterAttribute = base.GetIndexFieldNameFormatterAttribute(current);
                if (indexFieldNameFormatterAttribute != null)
                {
                    string indexFieldName = this.GetIndexFieldName(indexFieldNameFormatterAttribute.GetIndexFieldName(current.Name));
                    if (dictionary.ContainsKey(indexFieldName))
                    {
                        dictionary[indexFieldName].Add(indexFieldNameFormatterAttribute.GetTypeFieldName(current.Name));
                    }
                }
            }
            return dictionary;
        }
        // Sitecore.ContentSearch.LuceneProvider.LuceneFieldNameTranslator
        public override Dictionary<string, List<string>> MapDocumentFieldsToType(Type type, MappingTargetType target, IEnumerable<string> documentFieldNames)
        {
            if (target == MappingTargetType.Indexer)
            {
                return this.MapDocumentFieldsToTypeIndexer(type, documentFieldNames);
            }
            if (target == MappingTargetType.Properties)
            {
                return this.MapDocumentFieldsToTypeProperties(type, documentFieldNames);
            }
            throw new ArgumentException("Invalid mapping target type: " + target, "target");
        }

    }
}
