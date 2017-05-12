using System;
using System.Collections;
using System.Collections.Generic;

namespace Sitecore.Support.ContentSearch
{
    internal static class TypeActionHelper
    {
        public static void Call<T>(Action<T> action, params object[] instances) where T : class
        {
            TypeActionHelper.Call<T>(action, (IEnumerable<object>)instances);
        }

        public static void Call<T>(Action<T> action, IEnumerable<object> instances) where T : class
        {
            IEnumerable<T> enumerable = TypeActionHelper.FilterInstances<T>(instances);
            foreach (T current in enumerable)
            {
                action(current);
            }
        }

        private static IEnumerable<T> FilterInstances<T>(IEnumerable<object> instances) where T : class
        {
            foreach (object current in instances)
            {
                if (current != null)
                {
                    T t = current as T;
                    if (t != null)
                    {
                        yield return t;
                    }
                    else if (!(current is string))
                    {
                        IEnumerable enumerable = current as IEnumerable;
                        if (enumerable != null)
                        {
                            foreach (object current2 in enumerable)
                            {
                                t = (current2 as T);
                                if (t != null)
                                {
                                    yield return t;
                                }
                            }
                        }
                    }
                }
            }
            yield break;
        }
    }
}
