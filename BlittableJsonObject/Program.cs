using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NewBlittable;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;


namespace ConsoleApplication4
{
    class Program
    {
        public class Employee
        {
            public string Name;
            public int Age;
            public List<string> Dogs;
            public EOffice Office;

            public class EOffice
            {
                public string Name;
                public string Street;
                public string City;
            }
        }

      
        unsafe static void Main(string[] args)
        {
            var str = @"
{
 'Name': 'Oren',
 'Age': 34,
 'Dogs': ['Arava', 'Oscar', 'Phoebe'],
 'Office': {
     'Name': 'Hibernating Rhinos',
     'Street': 'Hanashia 21',
     'City': 'Hadera'
  }
}
";
  
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

                var bytes = new byte[size];
                for (var i = 0; i < size; i++)
                    bytes[i] = *(ptr++);
                File.WriteAllBytes($"out{DateTime.Now.Ticks}.txt", bytes);
            }
            
        }


    }
}
