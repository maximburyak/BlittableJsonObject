using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConsoleApplication4;
using NewBlittable.Tests.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Xunit;

namespace NewBlittable.Tests
{
   
    public unsafe class FunctionalityTests:BlittableJsonTestBase
    {
        [Fact]
        public void FunctionalityTest()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);
         
            var str = GenerateSimpleEntityForFunctionalityTest();
            using (var blittableContext = new BlittableContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                dynamic dynamicRavenJObject = new DynamicJsonObject(RavenJObject.Parse(str));
                dynamic dynamicBlittableJObject = new DynamicBlittableJson(ptr, employee.SizeInBytes, blittableContext);
                Assert.Equal(dynamicRavenJObject.Age, dynamicBlittableJObject.Age);
                Assert.Equal(dynamicRavenJObject.Name, dynamicBlittableJObject.Name);
                Assert.Equal(dynamicRavenJObject.Dogs.Count, dynamicBlittableJObject.Dogs.Count);
                for (var i = 0; i < dynamicBlittableJObject.Dogs.Length; i++)
                {
                    Assert.Equal(dynamicRavenJObject.Dogs[i], dynamicBlittableJObject.Dogs[i]);
                }
                Assert.Equal(dynamicRavenJObject.Office.Name, dynamicRavenJObject.Office.Name);
                Assert.Equal(dynamicRavenJObject.Office.Street, dynamicRavenJObject.Office.Street);
                Assert.Equal(dynamicRavenJObject.Office.City, dynamicRavenJObject.Office.City);
            }

        }

        [Fact]
        public void FunctionalityTest2()
        {
            byte* ptr;
            int size = 0;
            var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);

            var str = GenerateSimpleEntityForFunctionalityTest2();
            using (var blittableContext = new BlittableContext(unmanagedPool))
            using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
                "doc1"))
            {
                employee.Write();
                ptr = unmanagedPool.GetMemory(employee.SizeInBytes, string.Empty, out size);
                employee.CopyTo(ptr);

                AssertComplexEmployee(str, ptr, employee, blittableContext);
            }
        }

        

     
    }
}
