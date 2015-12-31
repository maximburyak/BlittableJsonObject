using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConsoleApplication4;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Xunit;

namespace NewBlittable.Tests.BlittableJsonWriterTests
{
    public unsafe class VariousPropertyAmountsTests
    {
        public ExpandoObject GenerateExpandoObject(int depth=1, int width=8, bool reuseFieldNames = true)
        {
            if (depth <= 0 || width<=0)
                throw new ArgumentException("Illegal depth or width");
            
            if (depth == 1)
            {
                var curExpando = new ExpandoObject();
                var cuExpandoAsDictionary = (IDictionary<string, object>) curExpando;
                for (var i = 0; i < width; i++)
                {
                    cuExpandoAsDictionary["Field" + i] = i.ToString();
                }

                return curExpando;
            }

           

            var expando = new ExpandoObject();
            var expandoAsDictionary = (IDictionary<string, object>)expando;
            for (var i = 0; i < width; i++)
            {
                expandoAsDictionary["Field" + i] = GenerateExpandoObject(depth - 1, width, reuseFieldNames);
            }

            return expando;
        }
        
        public string GetJsonString(int depth=1, int width=8, bool reuseFieldNames=true)
        {
            var expando = GenerateExpandoObject(depth,width,reuseFieldNames);

            JsonSerializerSettings serializerSettings = new JsonSerializerSettings();
            serializerSettings.Converters.Add(new StringEnumConverter());
            serializerSettings.Converters.Add(new ExpandoObjectConverter());

            JsonSerializer serializer = JsonSerializer.Create(serializerSettings);

            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            serializer.Serialize(jsonWriter, expando);
            return stringWriter.ToString();
        }

        public class CustomMemberBinder : GetMemberBinder
        {
            public CustomMemberBinder(string name, bool ignoreCase) : base(name, ignoreCase)
            {
            }

            public override DynamicMetaObject FallbackGetMember(DynamicMetaObject target, DynamicMetaObject errorSuggestion)
            {
                throw new NotImplementedException();
            }
        }

      //  [Fact]
        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue+1)]
        public void FlatBoundarySizeFieldsAmount(int maxValue)
        {
            //var maxValue = short.MaxValue + 1000;
            var str = GetJsonString(1, maxValue);

            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);
            
            using (var blittableContext = new BlittableContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                System.Dynamic.DynamicObject dynamicBlittableJObject = new DynamicBlittableJson(ptr,
                    employee.SizeInBytes, blittableContext);

                for (var i = 0; i < maxValue; i++)
                {
                    object curVal;
                    Assert.True(dynamicBlittableJObject.TryGetMember(new CustomMemberBinder("Field" + i, true), out curVal));
                    Assert.Equal(curVal.ToString(), i.ToString());
                }
            }
        }
    }
}
