using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using NewBlittable;
using NewBlittable.Tests;
using NewBlittable.Tests.Benchmark;
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
            //PerformanceAnalysis();
            //BigJsonsBenchmark.PerformanceAnalysis();
            ProfilerWork.Run(3);
            Console.WriteLine("Ready..");
            Console.ReadLine();
            ProfilerWork.Run(10);
            //new FunctionalityTests().LongStringsTest(100);
        }

        private static void PerformanceAnalysis()
        {
            using (var byteInAllHeapsConter = new PerformanceCounter(
                ".NET CLR Memory", "# Bytes in all Heaps", Process.GetCurrentProcess().ProcessName))
            using (var processProcessorTimeCounter = new PerformanceCounter(
            "Process", "% Processor Time", Process.GetCurrentProcess().ProcessName))
            using (var processPrivateBytes = new PerformanceCounter(
            "Process", "Private Bytes", Process.GetCurrentProcess().ProcessName))
            {
                var files = Directory.GetFiles(@"C:\Users\bumax_000\Downloads\JsonExamples", "*.json");

                foreach (var jsonFile in files)
                {
                    Console.WriteLine(jsonFile);
                    var jsonFileText = File.ReadAllText(jsonFile);
                    var jsonStringReader = new JsonTextReader(new StringReader(jsonFileText));

                    var before = GC.GetTotalMemory(true);
                    var sp = Stopwatch.StartNew();
                    while (jsonStringReader.Read())
                    {
                    }
                    var simpleReadTime = sp.ElapsedMilliseconds;
                    Console.WriteLine(
                        $"Simple JSON Read Time:{simpleReadTime:#,#} mem: {GC.GetTotalMemory(false) - before:#,#} bytes");
                    before = GC.GetTotalMemory(true);
                    sp.Restart();

                    JObject.Load(new JsonTextReader(new StringReader(jsonFileText)));
                    var jobjectReadTime = sp.ElapsedMilliseconds;

                    Console.WriteLine(
                        $"Json Object Read Time:{jobjectReadTime} mem: {GC.GetTotalMemory(false) - before:#,#} bytes");

                    before = GC.GetTotalMemory(true);

                    sp.Restart();

                    using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
                    using (var blittableContext = new BlittableContext(unmanagedPool))
                    using (
                        var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(jsonFileText)),
                            blittableContext,
                            "doc1"))
                    {
                        employee.Write();
                        var blittableObjectReadTime = sp.ElapsedMilliseconds;

                        Console.WriteLine($"Json Object Read Time:{blittableObjectReadTime} " +
                                          $"mem: {GC.GetTotalMemory(false) - before:#,#} bytes unmanaged {unmanagedPool.CurrentSize:#,#} bytes");
                    }
                }
            }
        }

        /*var file = @"C:\Users\bumax_000\Downloads\JsonExamples\file.txt";

        var str = File.ReadAllText(file);

        byte* ptr;
        int size = 0;
        var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024);


        //var jsonTextReader = new JsonTextReader(new StringReader(str));
        //while (jsonTextReader.Read())
        //{
        //}


        //JObject.Load(jsonTextReader);

        using (var blittableContext = new BlittableContext(unmanagedPool))
        using (var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(str)), blittableContext,
            "doc1"))
        {
            employee.Write();

            Console.WriteLine(new FileInfo(file).Length.ToString("#,#"));
            Console.WriteLine(employee.SizeInBytes.ToString("#,#"));

        }
     */
    }


}
