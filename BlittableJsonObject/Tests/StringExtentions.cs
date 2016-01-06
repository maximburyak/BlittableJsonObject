using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Abstractions.Extensions;

namespace NewBlittable.Tests
{
    public static class StringExtentions
    {
        public static string ToJsonString(this object self)
        {
            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, self);

            return stringWriter.ToString();
        }

        public static object GetValue(this MemberInfo memberInfo, object entity)
        {
            if (MemberInfoExtensions.IsProperty(memberInfo))
                return ((PropertyInfo)memberInfo).GetValue(entity, new object[0]);
            if (MemberInfoExtensions.IsField(memberInfo))
                return ((FieldInfo)memberInfo).GetValue(entity);
            throw new NotSupportedException("Cannot calculate CanWrite on " + (object)memberInfo);
        }

        public static bool IsProperty(this MemberInfo memberInfo)
        {
            return memberInfo.MemberType == MemberTypes.Property;
        }

        public static bool IsField(this MemberInfo memberInfo)
        {
            return memberInfo.MemberType == MemberTypes.Field;
        }
    }
}
